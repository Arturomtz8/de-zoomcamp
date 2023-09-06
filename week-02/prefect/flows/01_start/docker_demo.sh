prefect deployment build ./week-02/prefect/flows/01_start/homework_github_deploy.py:etl_web_to_gcs \
  -n docker-tutorial \
  -q test \
  -sb github/git-docker \
  -ib docker-container/docker-demo \
  -o prefect-docker-deployment \
  --apply


# {
#     "EXTRA_PIP_PACKAGES":  "pandas==1.5.2 prefect-gcp[cloud_storage]==0.2.4 protobuf==4.21.11 pyarrow==10.0.1 pandas-gbq==0.18.1"
# }
