"""Create diagrams using https://diagrams.mingrammer.com/"""
from diagrams import Cluster, Diagram
from diagrams.gcp.analytics import BigQuery, PubSub
from diagrams.gcp.compute import Functions
from diagrams.generic.storage import Storage as genericstorage
from diagrams.gcp.devtools import Scheduler
from diagrams.gcp.storage import Storage
from diagrams.custom import Custom

with Diagram("Toronto COVID-19 ELT Pipeline", show=False):
    pubsub = PubSub("PubSub topic")
    scheduler = Scheduler("Cloud Scheduler Job")
    
    datastudio = Custom("", "./data-studio.png")
    with Cluster("Extract"):
        storage = genericstorage("RPC-style API")
        function = Functions("Cloud Function")
    with Cluster("Load"):
        function_two = Functions("Cloud Function")
        cloud_storage = Storage("Cloud Storage")
    with Cluster("Transform"):
        bq = BigQuery("BigQuery")
        dbt = Custom("dbt", "./dbt_icon.png")
        
    scheduler >> pubsub >> function >> cloud_storage >> function_two >> bq >> datastudio
