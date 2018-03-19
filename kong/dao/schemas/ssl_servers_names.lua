local db_errors = require "kong.db.errors"
local Errors = require "kong.dao.errors"

return {
  table = "ssl_servers_names",
  primary_key = { "name" },
  fields = {
    name = { type = "text", required = true, unique = true },
    ssl_certificate_id = {
      type = "id",
      -- foreign = "ssl_certificates:id", -- done in self-check
    },
    created_at = {
      type = "timestamp",
      immutable = true,
      dao_insert_value = true,
      required = true,
    },
  },
  self_check = function(self, ssl_server_name_t, dao, is_update)
    local cert_id = ssl_server_name_t.ssl_certificate_id
    if cert_id ~= nil then
      local cert, err, err_t = dao.db.new_db.ssl_certificates:select({
        id = cert_id
      })
      if err then
        if err_t.code == db_errors.codes.DATABASE_ERROR then
          return false, Errors.db(err)
        end

        return false, Errors.schema(err_t)
      end

      if not cert then
        return false,
               Errors.foreign("no such SSL certificate (id=" .. cert_id .. ")")
      end
    end
  end,
}
