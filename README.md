# F1 Scenario
Simple pipeline using Apache Beam DirectRunner via Python SDK to find the top 3 average times by driver. 
The pipeline expects a 2 column CSV with the columns driver and time. 

## Getting Started
To quickly validate the task, you can use the provided make commands and Dockerfile
1. Initialise the container
    ```
    make build
    ```
2. Execute the tests
   ```
   make test
   ```
3. To run the pipeline to get the expected output of the sample file in the current directory
    ```
   make run
   ```

The pipeline output and input path can be overridden as necessary.
To verify this feature, run the container in an interactive shell and mount the current working directory as a 
volume using the following command:
```
make interactive
```