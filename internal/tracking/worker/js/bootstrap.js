"use strict";

// The host owns all capabilities.  This file deliberately exposes only a
// version marker; HTTP, cookies, storage and process access remain outside JS.
globalThis.__tracking_worker_bootstrap = Object.freeze({ version: 1 });
