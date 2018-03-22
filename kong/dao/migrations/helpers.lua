local json_decode = require("cjson.safe").decode
local cassandra = require("cassandra")


local _M = {}

local fmt = string.format
local table_concat = table.concat

-- Iterator to update plugin configurations.
-- It works indepedent of the underlying datastore.
-- @param dao the dao to use
-- @param plugin_name the name of the plugin whos configurations
-- to iterate over
-- @return `ok+config+update` where `ok` is a boolean, `config` is the plugin configuration
-- table (or the error if not ok), and `update` is an update function to call with
-- the updated configuration table
-- @usage
--    up = function(_, _, dao)
--      for ok, config, update in plugin_config_iterator(dao, "jwt") do
--        if not ok then
--          return config
--        end
--        if config.run_on_preflight == nil then
--          config.run_on_preflight = true
--          local _, err = update(config)
--          if err then
--            return err
--          end
--        end
--      end
--    end
function _M.plugin_config_iterator(dao, plugin_name)

  -- iterates over rows
  local run_rows = function(t)
    for _, row in ipairs(t) do
      if type(row.config) == "string" then
        -- de-serialize in case of Cassandra
        local json, err = json_decode(row.config)
        if not json then
          return nil, ("json decoding error '%s' while decoding '%s'"):format(
                      tostring(err), tostring(row.config))
        end
        row.config = json
      end
      coroutine.yield(row.config, function(updated_config)
        if type(updated_config) ~= "table" then
          return nil, "expected table, got " .. type(updated_config)
        end
        row.created_at = nil
        row.config = updated_config
        return dao.plugins:update(row, {id = row.id})
      end)
    end
    return true
  end

  local coro
  if dao.db_type == "cassandra" then
    coro = coroutine.create(function()
      local coordinator = dao.db:get_coordinator()
      for rows, err in coordinator:iterate([[
                SELECT * FROM plugins WHERE name = ']] .. plugin_name .. [[';
              ]]) do
        if err then
          return nil, nil, err
        end

        assert(run_rows(rows))
      end
    end)

  elseif dao.db_type == "postgres" then
    coro = coroutine.create(function()
      local rows, err = dao.db:query([[
        SELECT * FROM plugins WHERE name = ']] .. plugin_name .. [[';
      ]])
      if err then
        return nil, nil, err
      end

      assert(run_rows(rows))
    end)

  else
    coro = coroutine.create(function()
      return nil, nil, "unknown database type: " .. tostring(dao.db_type)
    end)
  end

  return function()
    local coro_ok, config, update, err = coroutine.resume(coro)
    if not coro_ok then return false, config end  -- coroutine errored out
    if err         then return false, err    end  -- dao soft error
    if not config  then return nil           end  -- iterator done
    return true, config, update
  end
end

do

  -- returns { { column_name = "example", type = "text", kind = "regular" }, ... }
  -- * column_name is the name of the column: id,
  -- * type is the cql type: string, int, uuid, list<string>, etc
  -- * kind is "regular" for non-pk columns, and "partition_key" or "clustering" pks
  local function get_column_definitions(db, table_name)
    local cql = fmt([[
      SELECT column_name, type, kind FROM system_schema.columns
      WHERE keyspace_name = '%s'
      AND table_name = '%s'
      ALLOW FILTERING;
    ]], db.cluster.keyspace, table_name)
    return db:query(cql, {}, nil, "read")
  end


  local function extract_keys(column_definitions)
    local partition_keys  = {}
    local partition_len   = 0
    local clustering_keys = {}
    local clustering_len  = 0
    for i, column in ipairs(column_definitions) do
      if column.kind == "partition_key" then
        partition_len = partition_len + 1
        partition_keys[partition_len] = column.column_name
      elseif column.kind == "clustering" then
        clustering_len = clustering_len + 1
        clustering_keys[clustering_len] = column.column_name
      end
    end

    return partition_keys, clustering_keys
  end


  local function create_partitioned_table(db,
                                          table_name,
                                          column_definitions_sans_partition)

    local partition_keys, _ = extract_keys(column_definitions_sans_partition)
    local primary_key_cql
    if #partition_keys > 0 then
      primary_key_cql = fmt(", PRIMARY KEY (partition, %s)", table_concat(partition_keys, ", "))
    else
      primary_key_cql = fmt(", PRIMARY KEY (partition)")
    end

    local declarations_sans_partition = {}
    for i, column in ipairs(column_definitions_sans_partition) do
      declarations_sans_partition[i] = fmt("%s %s", column.column_name, column.type)
    end
    local declarations_sans_partition_cql = table_concat(declarations_sans_partition, ", ")

    local cql = fmt("CREATE TABLE %s.%s(partition text, %s%s);",
                    db.cluster.keyspace,
                    table_name,
                    declarations_sans_partition_cql,
                    primary_key_cql)
    return db:query(cql, {}, nil, "write")
  end


  local function copy_partitioned_records(db,
                                          source_table_name,
                                          destination_table_name,
                                          column_definitions_sans_partition)

    local cql = fmt("SELECT * FROM %s.%s ALLOW FILTERING", db.cluster.keyspace, source_table_name)
    local coordinator = db:get_coordinator()
    for rows, err in coordinator:iterate(cql) do
      if err then
        return nil, err
      end

      for _, row in ipairs(rows) do
        local column_names = { 'partition' }
        local values = { cassandra.text(destination_table_name) }
        local len = 1

        -- first value is the partition, which defaults to the table name
        for _, col in ipairs(column_definitions_sans_partition) do
          local column_name = col.column_name
          local value = row[column_name]
          if value ~= nil then
            local type_converter = cassandra[col.type]
            if not type_converter then
              return nil, fmt("Could not find the cassandra type converter for column %s (type %s)",
                              column_name, col.type)
            end
            len = len + 1
            values[len] = type_converter(value)
            column_names[len] = column_name
          end
        end

        local question_marks = string.sub(string.rep("?, ", len), 1, -3)

        -- error(require('inspect')({ cql = insert_cql, values = values }))
        local insert_cql = fmt("INSERT INTO %s.%s (%s) VALUES (%s)",
                               db.cluster.keyspace,
                               destination_table_name,
                               table_concat(column_names, ", "),
                               question_marks)

        local _, err = db:query(insert_cql, values, nil, "write")
        if err then
          return nil, err
        end
      end
    end
  end


  local function drop_table(db, table_name)
    local cql = fmt("DROP TABLE %s.%s;", db.cluster.keyspace, table_name)
    return db:query(cql, {}, nil, "write")
  end


  local function is_already_partitioned(column_definitions)
    local partition_keys, clustering_keys = extract_keys(column_definitions)
    return #partition_keys > 0 and #clustering_keys > 0
  end


  function _M.partition_cassandra_table(dao, table_name)

    local db = dao.db
    local column_definitions, err = get_column_definitions(db, table_name)
    if err then
      return nil, err
    end

    if is_already_partitioned(column_definitions) then
      return nil, fmt("The table '%s' is already partitioned", table_name)
    end

    local aux_table_name = "aux_for_partition_of_" .. table_name

    local _, err = create_partitioned_table(db, aux_table_name, column_definitions)
    if err then
      return nil, err
    end

    local _, err = copy_partitioned_records(db, table_name, aux_table_name, column_definitions)
    if err then
      return nil, err
    end

    local _, err = drop_table(db, table_name)
    if err then
      return nil, err
    end

    local _, err = create_partitioned_table(db, table_name, column_definitions)
    if err then
      return nil, err
    end

    local _, err = copy_partitioned_records(db, aux_table_name, table_name, column_definitions)
    if err then
      return nil, err
    end

    local _, err = drop_table(db, aux_table_name)
    if err then
      return nil, err
    end
  end

end


return _M
