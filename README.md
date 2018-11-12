# Deploying a kubernetes CronJob with a Spark Cluster.

* 1. Install radanalytics.io `oc create -f https://radanalytics.io/resources.yaml`
* 2. Create the templates in the _openshift/_-folder, using `oc create -f <file>`.
* 3. Create the buildconfig and imagestream using _python36build-template.json_ and a git url, e.g. this repo. This should output your image which was built using _oshinko s2i_.
* 4. Deploy _Oshinko Web UI_, the template should exist in the project where you installed radanalytics.
* 5. When _Oshinko Web UI_ has been succesfully deployed, create a spark cluster which will be used by your CronJob. Name it something descriptive, e.g. **cronjob-cluster**.
* 6. Wait for the Spark Cluster to be deployed.
* 7. Create your cronjob using the _spark-cronjob-template.json_. **IMPORTANT**: Set the `OSHINKO_CLUSTER_NAME` parameter to the name of your Spark Cluster. CronJobs and ephemeral Spark Cluster doesn't seem to work.
* 8. Your CronJob will run with schedule you specified using the cluster you specified. 
# svvpoc-traffic-prep
