# glance-proxy

Proxy to provide image data from Openstack glance via HTTP, using an S3-like temporary store to support Range requests.

Main use case is to allow conversion of raw disk images stored in glance using `qemu-img convert` on a system
that does not have enough local disk space to accomodate the conversion. 

The server requires environment variables be set to provide login details for both Openstack and Minio (S3 client). 

Run the server on localhost:
```
export OS_AUTH_URL=https://os.example.com:5001/v2.0
export OS_USERNAME=example-user
export OS_PASSWORD=XXXXXXXXXXXXXXXX
export OS_TENANT_NAME=example-tenant
export MINIO_ENDPOINT=s3.amazonaws.com
export MINIO_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
export MINIO_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
glance-proxy -minio-bucket openstack-images -minio-prefix tmp
```

Example invocation of qemu-img:
```
qemu-img convert -O qcow2 --image-opts 'driver=http,timeout=900,url=http://127.0.0.1:8080/id/e50aefed-28a1-4118-bca5-32784626d51d' e50aefed-28a1-4118-bca5-32784626d51d.qcow2
```
