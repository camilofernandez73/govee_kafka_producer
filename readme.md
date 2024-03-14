1. Set ENV variable

    sudo nano /etc/environment

    GOVEE_KAFKA_PRODUCER_ENV=production


2. Set Up a Virtual Environment on the Raspberry Pi:

    cd /home/pi/projects/govee_kafka_producer/

    python3 -m venv venv

3. Activate the Virtual Environment:

    source venv/bin/activate

4. Install Dependencies:

    pip install -r requirements.txt

5. Running Application:

    /home/pi/projects/govee_kafka_producer/venv/bin/python /home/pi/projects/govee_kafka_producer/main_producer.py
