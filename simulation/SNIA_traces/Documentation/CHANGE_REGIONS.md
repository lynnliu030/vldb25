# Change regions

## Example
We change the regions: <br>
aws:eu-west-1  to azure:eastus <br>
aws:us-west-2 to  gcp:us-east1 <br>

Input file: IBMObjectStoreTrace009Part0.typeE.mc 
Output File: IBMObjectStoreTrace009Part0.typeE.aws_gcp_azure


```
python3 change_regions.py IBMObjectStoreTrace009Part0.typeE.mc IBMObjectStoreTrace009Part0.typeE.aws_gcp_azure aws:eu-west-1 aws:us-west-2 --dst_regions azure:eastus gcp:us-east1
```
 
# Example split base:

## For no base

6 regions 
Regions GET/PUT:
         aws:us-east-1,aws:us-west-1
         azure:eastus,azure:westus
         gcp:us-east1-b,gcp:us-west1-a

```
python3.11 split_regions.py ../IBMObjectStoreTrace003Part0.typeF.aws_gcp_azure ../IBMObjectStoreTrace003Part0.typeF.aws_gcp_azure_6regions aws:us-east-1 azure:eastus gcp:us-east1-b --dst_regions aws:us-east-1,aws:us-west-1 azure:eastus,azure:westus gcp:us-east1-b,gcp:us-west1-a
```

9 regions:
Regionsi GET/PUT:
         aws:us-east-1,aws:us-west-1,aws:eu-west-1 
         azure:eastus,azure:westus,azure:westeurope 
         gcp:us-east1-b,gcp:us-west1-a,gcp:europe-west1-b

```
python3.11 split_regions.py ../IBMObjectStoreTrace003Part0.typeF.aws_gcp_azure ../IBMObjectStoreTrace003Part0.typeF.aws_gcp_azure_9regions aws:us-east-1 azure:eastus gcp:us-east1-b --dst_regions aws:us-east-1,aws:us-west-1,aws:eu-west-1 azure:eastus,azure:westus,azure:westeurope gcp:us-east1-b,gcp:us-west1-a,gcp:europe-west1-b

```


# For with base -> base: aws:us-east-1

4 Regions:
       PUT:
         aws:us-east-1
       GET:
         azure:eastus,azure:westus
         gcp:us-east1-b,gcp:us-west1-a

```
python3.11 split_regions.py ../IBMObjectStoreTrace003Part0.typeE.aws_gcp_azure ../IBMObjectStoreTrace003Part0.typeE.aws_gcp_azure_4regions aws:us-east-1 azure:eastus gcp:us-east1-b --dst_regions aws:us-east-1,aws:us-east-1 azure:eastus,azure:westus gcp:us-east1-b,gcp:us-west1-a,

```

7 Regions:
       PUT:
         aws:us-east-1
       GET:
         azure:eastus,azure:westus,azure:westeurope 
         gcp:us-east1-b,gcp:us-west1-a,gcp:europe-west1-b


```
python3.11 split_regions.py ../IBMObjectStoreTrace003Part0.typeE.aws_gcp_azure ../IBMObjectStoreTrace003Part0.typeE.aws_gcp_azure_7regions aws:us-east-1 azure:eastus gcp:us-east1-b --dst_regions aws:us-east-1,aws:us-east-1,aws:us-east-1 azure:eastus,azure:westus,azure:westeurope gcp:us-east1-b,gcp:us-west1-a,gcp:europe-west1-b
```
