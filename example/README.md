# Example Cluster Job

This project aims to get a simple PySpark job running on the cluster. The Python and csv data files were pulled from the AWS [tutorial](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html) on using PySpark and the cluster.

### Google Cloud Platform (GCP)

To get started with GCP, here are some lessons learned:
1. Install gcloud CLI and gsutil by following the instructions [here](https://cloud.google.com/storage/docs/gsutil_install).
2. Enable APIs. I believe Cloud Dataproc and Cloud Resource Manager were the minimumm required here.
3. Create buckets on Google Cloud Storage through the web console.
4. Upload data to buckets. Useful flags are -r (recursive) and -m (parallel operation). E.g. `gsutil -m cp -r health_violations.py gs://BUCKET/`
5. Create a cluster through the web console. I initially attempted to do it through the command line, but there were numerous errors to dig though and comparatively the console was quite simple.
6. Submit a job via the web console. When including arguments, the syntax is:
```bash
--data_source=gs://BUCKET/food_establishment_data.csv
--output_uri=gs://BUCKET/output/
```
7. Submit a job via the command line. Note that `--` separates arguments to the job submission from arguments to the code:
```bash
gcloud dataproc jobs submit pyspark --cluster CLUSTER --region us-central1 gs://BUCKET/health_violations.py -- --data_source=gs://BUCKET/food_establishment_data.csv --output_uri=gs://BUCKET/output/
```
8. Stop and delete clusters when finished.

