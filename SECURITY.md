# Security

Report a problem to **privacy@cordata.tech**, or through a private advisory on this
repository. Expect an acknowledgement within three working days.

What the lab can reach: the containers in `compose.yaml`, on a network of their own,
with every published port bound to `127.0.0.1`. The emulators use fixed, published
credentials (`lab/emulators.env`), which grant access to fictitious data on this
machine and to nothing else. No container is given a real cloud credential, and none
should be: a change that adds one is a security problem in itself.
