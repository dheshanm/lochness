# Azurite

Azurite is an open-source Azure Storage API compatible server (emulator). It supports most of the commands supported by Azure Storage with some limitations. 

## Resources

- [GitHub Repository](https://github.com/Azure/Azurite)
- [Documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite)

## Docker

Please use the included `docker-compose.yml` file to run Azurite in a Docker container. This setup includes the necessary configurations to get a test Azurite up and running quickly.

```bash
docker-compose up -d
```

## Connecting to Azurite
You can connect to the Azurite server using the following default connection string:

```
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;
```

If trying to connect from a different machine, replace `localhost` with the appropriate IP address or hostname.

## Azure Storage Explorer

You can use [Azure Storage Explorer](https://azure.microsoft.com/en-us/products/storage/storage-explorer) to interact with your Azurite instance. When setting up a new connection, use the same connection string provided above.
