import os
import requests
from dotenv import load_dotenv
import psycopg2


def main():
    # Load .env variables
    load_dotenv()  # reads .env from current directory

    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )

    strapi_password = os.getenv("STRAPIPW")
    strapi_name = os.getenv("STRAPIID")
    strapi_url = os.getenv("STRAPIURL")

    def getStrapiJwt():
        response = requests.post(
            strapi_url + "/auth/local",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json={"identifier": strapi_name, "password": strapi_password},
        )
        response.raise_for_status()

        jsonResponse = response.json()
        return jsonResponse["jwt"]

    strapiJwt = getStrapiJwt()

    def getGpuClasses():
        response = requests.get(
            strapi_url + "/gpu-classes",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Bearer " + strapiJwt,
            },
        )
        jsonResponse = response.json()
        output = {}
        for j in jsonResponse:
            output[j["uuid"]] = j
        return output

    published_gpu_classes = getGpuClasses()

    # Insert/update published_gpu_classes into the table
    with conn:
        with conn.cursor() as cur:
            for uuid, gpu in published_gpu_classes.items():
                cur.execute(
                    """
                    INSERT INTO gpu_classes (
                        gpu_class_id, batch_price, low_price, medium_price, high_price, gpu_type, gpu_class_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (gpu_class_id) DO UPDATE SET
                        batch_price = EXCLUDED.batch_price,
                        low_price = EXCLUDED.low_price,
                        medium_price = EXCLUDED.medium_price,
                        high_price = EXCLUDED.high_price,
                        gpu_type = EXCLUDED.gpu_type,
                        gpu_class_name = EXCLUDED.gpu_class_name
                    """,
                    (
                        uuid,
                        gpu.get("batchPrice"),
                        gpu.get("lowPrice"),
                        gpu.get("mediumPrice"),
                        gpu.get("highPrice"),
                        gpu.get("gpuClassType"),
                        gpu.get("name"),
                    ),
                )

    # Load all gpu_classes from the table and save as a dictionary
    gpu_classes_dict = {}
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT gpu_class_id, gpu_class_name FROM gpu_classes")
            for row in cur.fetchall():
                gpu_classes_dict[row[0]] = row[1]
    # Now gpu_classes_dict maps uuid to readable name


if __name__ == "__main__":

    main()
