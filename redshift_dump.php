<?php

class redshift_dump
{


    public function __construct()
    {
        $this->copyData();
        
    }

    private function copyData()
    {
        $query = "SELECT * FROM TABLE";
        $this->runQuery([
            "unload ('{$query}')",
            "to 's3://s3bucket/folder/'",
            "credentials 'aws_access_key_id=awsaccesskey;aws_secret_access_key=/secretaccesskey'",
            "DELIMITER as ','",
            "ADDQUOTES",
            "PARALLEL OFF",
            "GZIP;"
        ]);
    }


    public function runQuery(array $sql)
    {
        $sql = implode(PHP_EOL, $sql);
        $db = pg_connect(
            "host=redshiftconnection.amazonaws.com dbname=databasename user=username password=password port=5439"
        ) or die('Could not connect: ' . pg_last_error());

        $result = pg_query(
            $sql
        ) or die('Query failed: ' . pg_last_error());

        $return = [];

        while ($line = pg_fetch_array($result, null, PGSQL_ASSOC)) {
            $row = [];
            foreach ($line as $key => $value) {
                $row[$key] = $value;
            }
            $return[] = $row;
        }

        pg_free_result($result);
        pg_close($db);

        return $return;

    }
}

new redshift_dump();
