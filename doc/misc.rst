Misc
====

Backward incompatible change
----------------------------
Git commit b8dc82e4 (January 2016) introduced a major backward incompatible
change in how a Swift object (identified by its name) is mapped into a Sproxyd
path. As such any Swift object put with a version of `swift-scality-backend`
that does not have commit b8dc82e4 will be unreachable once
`swift-scality-backend` is upgraded.

This is only a concern for non-POC installations running
`swift-scality-backend` version 0.3 that would like to upgrade to a newer
version of `swift-scality-backend`. In that case a proper migration strategy
would have to be put in place.

Mapping of Swift names to Sproxyd paths
---------------------------------------
In OpenStack Swift, the canonical URL to an object is 
http://swiftproxyhost/account/container/object where `account` is the account
ID of the user, `container` is the container name and `object` the object name.
`swift-scality-backend` requires Sproxyd to be configured to accept queries "by 
path". The Sproxyd path is derived by SHA1-hashing the concatenation of
`account`, `container` and `object`. This way, the Swift object can be retrieved
directly though Sproxyd at this location:
http://sproxydhost/proxy/namespace/SHA1(account+container+object)

**N.B**: `namespace` is usually "bpchord" or "bparc".

This is useful to debug a Swift installation that use the Scality backend.


