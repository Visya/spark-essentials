package part4sparksql

object SparkShell extends App {
  /*
    spark-sql>...
    show databases;
    create database rockthejvm;
    use rockthejvm;
    create table persons(id integer, name string); // creates managed table
    select * from persons;
    insert into persons values (1, "Martin Odersky"), (2, "Matei Zaharia");
    describe extended persons;
    create table flights(origin string, destination string) using csv options(header true, path "/home/rockthejvm/data/flights"); // creates external(unmanaged) table
  */
}
