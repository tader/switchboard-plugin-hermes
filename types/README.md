# Minimal independently authored compatibility type

`api.d.ts` is **not a copy of the Switchboard SDK**. It declares only the `dataDir: string` setup-context member consumed by this implementation, under the MIT license in [LICENSE](LICENSE). No uncommitted local contract declarations or upstream implementation text are vendored. The plugin is JavaScript; actual service registration is validated by the released core in the socket fixture.

Verified primary release SDK: [Switchboard 0.9.0, api.ts at af10250cab41991ae5f5214de1222bab3fd7de71](https://github.com/tader/switchboard/blob/af10250cab41991ae5f5214de1222bab3fd7de71/server/plugins/api.ts). The immutable SDK blob is `2e9ee8147862a0190ec7632b19089c5a8799d1f0`.

Minimum-version reference: [Switchboard 0.8.3 at ec6fbc36abc25e087f44191d90c4572790acf152](https://github.com/tader/switchboard/blob/ec6fbc36abc25e087f44191d90c4572790acf152/server/plugins/api.ts).

Only these existing extension fields/hooks are used:

* Setup context `dataDir` for durable plugin state.
* Returned instance `services` and `dispose()`.
* Service `id`, `name`, `description`, `baseUrl`, `allowedHosts`, `openapi`, `authMethods`.
* Auth method `id`, `name`, `fields`, `connect`, `authorize`, `revoke`.
* Field `key`, `label`, `type`, `required`, `default`, `description`.
* Connected result `config`, `credentials`, `account`; account `id`, `label`.
* Authorized request `method`, `url`, `headers`; connection `id`, `config`, `credentials`.

These are structural compatibility facts, not new SDK methods. The plugin does not use shared-contract additions, custom lifecycle notifications, arbitrary HTTP registration or a fabricated SSE/WebSocket capability. Full upstream SDK/implementation source is not redistributed: the integration fixture exports each immutable release only into disposable test storage from an explicitly supplied local repository.
