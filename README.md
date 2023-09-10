# MySQL to NoSMongoDB Sync App

## Project Description
This project facilitates the efficient synchronization between MySQL and NoSMongoDB databases. It is developed using Java and encompasses the code utilized in the tests featured in the paper titled "Implementing a Synchronization Method between a Relational and a Non-Relational Database".

## How to Use

Once all the necessary services (MySQL, MongoDB, and Kafka) have been set up, you can use the application to create a sync system between your SQL and NoSQL databases.

## Setup Requirements

To run this application, ensure you have MongoDB, MySQL, and Kafka deployed on the same machine. The Java environment is also a prerequisite for running this application. Here are the detailed setup requirements:

- Java JDK (version 8 or above)
- MongoDB
- MySQL
- Apache Kafka

## Running the Tests

To verify the setup and working of the application, you should run the predefined tests. Follow these steps:

1. Start the MongoDB, MySQL, and Kafka services.
2. Set up your Java environment and compile the application using a suitable Java IDE or the command line.
3. Execute the tests available in the `org.disertatie.dbsync.test` directory using the appropriate REST endpoints.
   
You can observe the logs and output to verify the successful synchronization of data between the SQL and NoSQL databases.

## Miscellaneous

- Ensure to check the configuration files and update the database credentials and other necessary details before running the application.

## Additional Information

- The project is open to contributions; feel free to raise issues or submit pull requests on the repository page.

For further details, you can refer to the paper titled "Implementing a Synchronization Method between a Relational and a Non-Relational Database" to get an insight into the testing scenarios and results obtained during the development of this application.

Thank you for using the SQL to NoSQL Database Sync App.