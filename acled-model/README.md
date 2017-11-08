This model downloads the full acled dataset through api.
It removes duplicates and add to the Postgres SQL database only the events not already exported.
The only input parameter is the input "year" for the IDMC_ACLED_downloader transformer

In order to run the model we need to run a ssh tunner to the port 6000:
ssh -L 6000:localhost:5433 idetect@139.162.131.29
