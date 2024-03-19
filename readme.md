
## Govee Kafka Producer

Lee archivos generados por GoveeBTTempLogger y los manda a un servidor de kafka.
Principalmente diseñado para realizar pruebas de carga en un servidor Kafka en una Raspberry pi 4.

## Setup:

1. **Set ENV variable**
    ```
      sudo nano /etc/environment

      GOVEE_KAFKA_PRODUCER_ENV=production
    ```

2. **Set Up a Virtual Environment on the Raspberry Pi:**
    ```
    cd /home/pi/projects/govee_kafka_producer/

    python3 -m venv venv
    ```
3. **Activate the Virtual Environment**:
    ```
    source venv/bin/activate
    ```
4. **Install Dependencies:**
    ```
    pip install -r requirements.txt
    ```
5. **Running Application:**
    ```
    /home/pi/projects/govee_kafka_producer/venv/bin/python /home/pi/projects/govee_kafka_producer/main_producer.py
    ```


## Running as a Service
Creating a systemd service one of the most robust and common ways to ensure a Python script runs at boot and restarts on failure.

### Creating a systemd Service Unit File

1. **Create a Unit File**: 
    
    Create a unit file for the service: 
    ```sh
    sudo nano /etc/systemd/system/govee_kafka_producer.service
    ```

2. **Define the Service**: 

    Add the following configuration to the file. Adjust paths according to the Python script and virtual environment:

    ```
    [Unit]
    Description=Govee Kafka Producer Service
    After=network.target

    [Service]
    Environment="GOVEE_KAFKA_PRODUCER_ENV=production"
    User=pi
    WorkingDirectory=/home/pi/path/to/your/script
    ExecStart=/home/pi/path/to/your/venv/bin/python path/to/your/script/main_producer.py
    Restart=always
    RestartSec=30

    [Install]
    WantedBy=multi-user.target
    ```

    - `Description` is a meaningful description of your service.
    - `After=network.target` ensures the network is available before starting your service.
    - `User` is the user under which the script should run. 
    - `WorkingDirectory`  where the Python script is located.
    - `ExecStart` full path to the Python executable in the virtual environment and the path to the script.
    - `Restart=always` restart the service if it fails.
    - `RestartSec=30` restart after 30 seconds if it fails.

3. **Enable and Start the Service**: 

    - Reload systemd to read the new unit file:

      ```
      sudo systemctl daemon-reload
      ```

    - Enable the service to start on boot:

      ```
      sudo systemctl enable govee_kafka_producer.service
      ```

    - Start the service immediately:

      ```
      sudo systemctl start govee_kafka_producer.service
      ```

 
4. **Additional Commands**: 

    - Check status, to verify the service is running correctly:
      ```
      sudo systemctl status govee_kafka_producer.service
      ```

    - Stop the service immediately:
      ```
      sudo systemctl stop govee_kafka_producer.service
      ```

    - Restart: Stop and immediately start the service again:
      ```
      sudo systemctl restart  govee_kafka_producer.service
      ```
    - Disable: Prevent the service from starting automatically at boot, use:
      ```
      sudo systemctl disable  govee_kafka_producer.service
      ```

6.  Viewing Logs

    To troubleshoot or check the output of the script:

    ```sh
    journalctl -u govee_kafka_producer.service
    ```

    Use the -e option, which takes you to the end of the journal:

    ```sh
    journalctl -u govee_kafka_producer.service -e
    ```




##  Running as Cron `@reboot` Job

Use cron's @reboot feature to run scripts at startup. This method is simpler than creating a systemd service but lacks some of the advanced management features like automatic restarts on failure.

**How to Set Up**:
  1. Open the crontab file for editing: `crontab -e`.
  2. Add a line for your script using the `@reboot` directive: `@reboot /path/to/your/env/bin/python /path/to/your/script.py`.
  3. Save and close the editor.

