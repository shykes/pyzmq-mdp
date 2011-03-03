MDP - the Majordomo Protocol
============================

An implementation in Python using pyzmq.

For the specification of see
<center>
<a href="http://rfc.zeromq.org/spec:7">MDP - the Majordomo Protocol<a/>
</center>

The implementation will make use of the more advanced parts of the
pyzmq bindings, namely ioloop and streams.

Because of this, most of the code will *not* look like the examples in
the guide. The examples in the guide are more or less direct
translations of the C examples. But pyzmq does offer more than a
shallow wrapper of the C API.

