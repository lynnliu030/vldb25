## Object store benchmark 
Run `pip install skyatc`, and then `python benchmark/instance_obj_store.py`. 

Make sure to create a `benchmark/config.json` file with the following format: 
```
{
    "cloudflare": {
        "account_id": "",
        "access_key": "", 
        "secret_key": ""
    }, 
    "aws": {
        "access_key": "", 
        "secret_key": ""
    }, 
    "gcp": {}
}
```

## Dockerfile 
Run: 
```
sudo docker build -t sarahwooders/benchmark .
docker push sarahwooders/benchmark:latest
```
Run benchmark with: 
```
docker run 
```

