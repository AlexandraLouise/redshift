<?php

class Primary
{
    private $xml_file = '/tmp/redshift-dump.xml';
    private $json_file = '/tmp/redshift-dump.json';
    private $json_file_compressed = '/tmp/redshift-dump.json.gz';
    private $json_file_s3 = 's3://folder/redshift-dump.json.gz';

    public function __construct()
    {
     $this->processTable(date('Y-m-d', strtotime('yesterday')));
    }

  

 private function processCountries($target_date)
    {
        $this->process('TableName', sprintf("date = '%s'", $target_date));
        $this->runQuery([
           'UPDATE TableName
            set
            id                  = s.id,
            column              = s.column,
            created_at          = s.created_at,
            updated_at          = s.updated_at
            FROM staging_tablename  s
            WHERE s.id = tablename.id',
        ]);

        $this->runQuery([
            'INSERT INTO TableName
            SELECT
                s.id,
                s.column,
                s.created_at,
                s.updated_at
            FROM staging_tablename s
            LEFT JOIN tablename s2 on s.id = s2.id
            WHERE s2.id IS NULL',
        ]);
        $this->write_logs("TableName",  $target_date);
    }




    private function write_logs($import_description, $import_date) {
        $this->runQuery([
            sprintf("INSERT INTO import_logs
              (created, import_date, import_description)
              VALUES
              (getdate(), '%s', '%s')", $import_date,$import_description)
        ]);
        $this->info('Wrote to Log');
    }



    private function process($table, $query)
    {
        $this->info('Starting ' . $table . ' with query ' . $query);
        $this->dumpData($table, $query);
        $this->compressJson();
        $this->moveToS3();
        $this->copyData($table);
        $this->info('Finished ' . $table . ' with query ' . $query);
    }


    private function copyData($table)
    {
        $staging_table = 'staging_' . $table;

        $this->runQuery(['DROP TABLE IF EXISTS ' . $staging_table]);
        $this->runQuery([
            sprintf('CREATE TABLE %s (LIKE %s)', $staging_table, $table),
        ]);

        $this->runQuery([
            "copy " . $staging_table,
            sprintf("from '%s'", $this->json_file_s3),
            "credentials 'aws_access_key_id=123456*;aws_secret_access_key=#fjask52%'",
            "json 'auto' gzip",
        ]);
    }


    /**
     * @param $table
     * @param $query
     * @return bool If there was data returned
     */
    private function dumpData($table, $query)
    {
        if ( file_exists($this->xml_file) ) {
            unlink($this->xml_file);
        }
        system(sprintf(
            'mysqldump -h instancename.cc2345677.eu-west-1.rds.amazonaws.com -u aurora_user -aurora_password --xml database_name %s --where="%s"> %s',
            $table,
            $query,
            $this->xml_file
        ));

        $this->mysqlDumpXmlToJson();

        return (filesize($this->json_file) > 1000);
    }

    private function mysqlDumpXmlToJson()
    {
        if ( file_exists($this->json_file) ) {
            unlink($this->json_file);
        }

        $z = new \XMLReader();
        $z->open($this->xml_file);

        $doc = new \DOMDocument();

        while ($z->read() && $z->name !== 'row');

        $f = fopen($this->json_file, 'a+');

        while ($z->name === 'row')
        {
            $data = [];
            $node = simplexml_import_dom($doc->importNode($z->expand(), true));

            foreach ( $node as $col ) {
                $value = (string) $col;
                $value = str_replace('0000-00-00 00:00:00', '', $value);
                $data[(string) $col['name']] = $value;
            }

            fwrite($f, json_encode($data));

            $z->next('row');
        }

        fclose($f);
    }

    private function compressJson()
    {
        system('gzip ' . $this->json_file);
    }

    private function moveToS3()
    {
        system(sprintf('aws s3 mv %s %s', $this->json_file_compressed, $this->json_file_s3));
    }

    public function runQuery(array $sql)
    {
        $sql = implode(PHP_EOL, $sql);
        $db = pg_connect(
            "host=instancename.cc2345677.eu-west-1.rds.amazonaws.com dbname=database_name user=redshift_user password=redshift_password port=5439"
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

    public function info($msg)
    {
        echo date('r') . '---' . $msg . PHP_EOL;
    }
}

new Primary();