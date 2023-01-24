# Week 1: GCP + Terraform

Guiding references:
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp

## Homework

Homework guide: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_terraform/homework.md

### GCP credentials

- Option 1 (recommended): Create a service account for provisioning resources as presented in both course video and instructions:
    - [DE Zoomcamp 1.3.1 - Introduction to Terraform Concepts & GCP Pre-Requisites](https://www.youtube.com/watch?v=Hajwnmj0xfQ&t=326s)
    - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md#setup-for-access

- Option 2: Use Cloud Shell where authorization request will be prompted for Google account with enough permissions (usually editor role).

- Option 3: Login into Google account with enough permissions (usually editor role) for getting application default credentials into local machine:
    ```bash
    gcloud auth application-default login
    ```

### Provisioning with Terraform
- [main.tf](./main.tf) is a copy of guiding reference, the only adition is `terraform.required_providers.google.version` field and the only deletion is `terraform.required_version` field.
- [variables.tf](./variables.tf) is a copy of guiding reference, the only change is setting the default region to "us-central1".

Define GCP project:
```bash
# manually define project
GCP_PROJECT_ID=

# if working from Cloud Shell then use `DEVSHELL_PROJECT_ID` variable
GCP_PROJECT_ID="$DEVSHELL_PROJECT_ID"
```

Initialize and plan provisioning:
```bash
terraform init

terraform plan -var project="$GCP_PROJECT_ID"
```

Provision resources:
```bash
terraform apply -var project="$GCP_PROJECT_ID"
```

Destroy resources:
```shell
terraform destroy -var project="$GCP_PROJECT_ID"
```
