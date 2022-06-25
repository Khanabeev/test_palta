import json
import logging
import sqlite3
import random
import asyncio

from storage.storage import Storage


def db_init(conn: Storage):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS EVENTS
             (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                EVENT_ID INT NOT NULL UNIQUE,
                CLIENT_ID CHAR(50),
                PAYLOAD CHAR(50)
                );
             """)


def store_executed_event(conn: Storage, json_object):
    crs = conn.get_cursor()
    crs.execute("""
    INSERT INTO EVENTS (EVENT_ID, CLIENT_ID, PAYLOAD)
      VALUES (?,?,?)
             """, (json_object["id"], json_object["client_id"], json_object["payload"]))
    conn.commit()


async def main(config, conn):
    file = open("data/requests.txt", "r")
    # Read events from file
    tasks = []
    for line in file.readlines():
        event = json.loads(line.rstrip())

        if "id" not in event:
            logging.warning(f"id is not defined: {line}")
            continue

        task = asyncio.create_task(make_post_request(config[event['client_id']], event, conn))
        tasks.append(task)

    await asyncio.gather(*tasks)

    file.close()


async def make_post_request(endpoints, event, conn):
    print(f"Event: {event}")
    post_timeout = 1
    success_request = False

    while not success_request:
        for endpoint in endpoints:
            print(f"Post timeout: {post_timeout}")
            await asyncio.sleep(post_timeout)
            # Request
            if random.randint(0, 1):
                try:
                    # successful request
                    store_executed_event(conn, event)
                    success_request = True
                    break
                except sqlite3.IntegrityError:
                    print(f"id already exists: {event['id']}")
                    success_request = True
            else:
                # failed request
                post_timeout += 1


if __name__ == '__main__':
    conn = Storage("database")
    logging.basicConfig(
        filename='logs/app.log',
        filemode='w',
        format='%(name)s - %(levelname)s - %(message)s')

    with open("config/config.json", "r") as file:
        config = json.load(file)

    db_init(conn)

    asyncio.run(main(config, conn))

    conn.close_connection()
