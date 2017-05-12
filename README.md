Usually for syncronisations between RDS and Redshift, we could set up data pipelines which were relatively simple (albeit finicky!) to set up. However after moving to an Aurora database we had to find a way to transfer the data between these two databases without data pipelines as Amazon does not yet have functionality available for this transfer in the Data Pipeline product. 

This is the PHP script we use in order to syncronize one table. 
