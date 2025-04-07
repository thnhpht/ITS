import base64
import importlib.util
import json
import logging.handlers
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from threading import Thread
from urllib.parse import urlparse
import pandas as pd

import pika
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from flask_httpauth import HTTPBasicAuth
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, decode_token
from urllib.parse import urlparse

from get_email_from_jodcode import get_email_by_jodcode

# Đường dẫn đến py_common.py
# lib_file_path = "./py_common.py"
# base_dir = "./"
# dir_config = "./"
lib_file_path = "/home/Python/py_common.py"
base_dir = "/home/Python/OutBoxTrigger/ITS"
dir_config = "/home/Python"
lib_file_mapping_path = "/home/Python/xuly_luong_ticket_item/mapping_ho_auto.py"

spec = importlib.util.spec_from_file_location("py_common", lib_file_path)
py_common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(py_common)

spec_mapping = importlib.util.spec_from_file_location("mapping_ho_auto", lib_file_mapping_path)
mapping_ho = importlib.util.module_from_spec(spec_mapping)
spec_mapping.loader.exec_module(mapping_ho)


def load_config():
    try:
        config_1 = py_common.read_config(os.path.join(dir_config, "common_config", "config.ini"))
        API_HOST1 = config_1["its_config"]["ITS_HOST"]
        ENV = config_1["its_config"]["ENV"]
        LOG_CURL = bool(config_1["its_config"]["LOG_CURL"])
        TOKEN_CREDENTIAL = {
            "URL": config_1["its_config"]["TOKEN_URL"],
            "USERNAME": config_1["its_config"]["TOKEN_USERNAME"],
            "PASSWORD": config_1["its_config"]["TOKEN_PASSWORD"]
        }
        AUTH_CREDENTIAL = {
            "USERNAME": config_1["its_config"]["AUTH_USERNAME"],
            "PASSWORD": config_1["its_config"]["AUTH_PASSWORD"],
            "TIME_EXPIRED": int(config_1["its_config"]["JWT_TIME_EXPIRED"]),
            "SECRET_KEY": config_1["its_config"]["JWT_SECRET_KEY"],
        }
        RABBITMQ_CONFIG = {
            "HOST": config_1["rabbitmq"]["host"],
            "PORT": int(config_1["rabbitmq"]["port"]),
            "USERNAME": config_1["rabbitmq"]["username"],
            "PASSWORD": config_1["rabbitmq"]["password"],
            "QUEUE_NAME": config_1["rabbitmq"]["lpbQueue"],
        }
        api_its = config_1["api_its"]
        api_ho = config_1["api_ho"]
        API_PATH = {
            "API_ITS": {
                "GET_SERVICE_CATEGORY": api_its["service_category"],
                "GET_TEMPLATE_BY_SERVICE_CATE": api_its["template_by_service_cate"],
                "GET_TEMPLATE": api_its["template"],
                "GET_CATEGORY": api_its["category"],
                "GET_SUB_CATEGORY": api_its["sub_category"],
                "GET_ITEM": api_its["item"],
                "CREATE_REQUEST": api_its["create_request"],
                "UPDATE_REQUEST": api_its["update_request"],
            },
            "API_HO": {
                "GET_SERVICE_CATEGORY": api_ho["service_category"],
                "GET_TEMPLATE_BY_SERVICE_CATE": api_ho["template_by_service_cate"],
                "GET_TEMPLATE": api_ho["template"],
                "GET_CATEGORY": api_ho["category"],
                "GET_SUB_CATEGORY": api_ho["sub_category"],
                "GET_ITEM": api_ho["item"],
                "CREATE_REQUEST": api_ho["create_request"],
                "UPDATE_REQUEST": api_ho["update_request"],
            }
        }

        SETTINGS = {
            "MAX_RETRIES": int(config_1["its_config"]["MAX_RETRIES"]),
            "RETRY_DELAY": int(config_1["its_config"]["RETRY_DELAY"]),
            "PATH_DATA": "./data.json",
        }

        return (API_HOST1, TOKEN_CREDENTIAL, AUTH_CREDENTIAL, SETTINGS, RABBITMQ_CONFIG, ENV, LOG_CURL, API_PATH, config_1)
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return (None, None, None, None, None, None, None, None, None)


def config_log():
    try:
        log_dir = os.path.join(base_dir, "logs")

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_filepath = os.path.join(log_dir, "log.txt")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(log_filepath, maxBytes=20 * 1024 * 1024, backupCount=4),
                logging.StreamHandler()
            ])

        # Giảm mức độ log của Pika xuống WARNING
        pika_logger = logging.getLogger("pika")
        pika_logger.setLevel(logging.WARNING)

        # Nếu muốn tắt hoàn toàn log của Pika, thay dòng trên bằng:
        # pika_logger.disabled = True

        return logging.getLogger(__name__)

    except Exception as e:
        logging.error(f"Error: {e}")
        return None
    

def create_app(secret_key, time_expired):
    """
    Creates and configures a Flask application instance.

    Parameters:
        secret_key (str): The secret key for the application.
        time_expired (int): Token expiration time in seconds.

    Returns:
        Flask: Configured Flask application instance.
    """
    try:
        app1 = Flask(__name__)

        app1.config['SECRET_KEY'] = secret_key
        app1.config['JWT_SECRET_KEY'] = secret_key
        app1.config['JWT_TOKEN_LOCATION'] = ['headers']
        app1.config['JWT_HEADER_NAME'] = 'Authorization'
        app1.config['JWT_HEADER_TYPE'] = 'Bearer'
        app1.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(seconds=time_expired)

        jwt_app = JWTManager(app1)
        basic_auth_app = HTTPBasicAuth()
        cors_app = CORS(app1)

        return app1, jwt_app, basic_auth_app, cors_app

    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None, None, None
    

try:
    proxies = {"http": None, "https": None}

    cached_token = None
    token_timestamp = None
    (
        API_HOST, TOKEN_CREDENTIAL, AUTH_CREDENTIAL, SETTINGS, RABBITMQ_CONFIG, ENV, LOG_CURL, API_PATH,
        config) = load_config()

    ITS_HOST_IMAGE = config["its_config"]["ITS_HOST_IMAGE"]
    CRM_HOST_FILE_TICKET = config["its_config"]["CRM_HOST_FILE_TICKET"]
    CRM_TEXT_REPLACE = config["its_config"]["CRM_TEXT_REPLACE"]
    # CRM_HOST_IMAGE_TICKET_TMP = config["its_config"]["CRM_HOST_IMAGE_TICKET_TMP"]

    app, jwt, basic_auth, cors = create_app(AUTH_CREDENTIAL["SECRET_KEY"], AUTH_CREDENTIAL["TIME_EXPIRED"])
    logger = config_log()
    logger.info("Application started")
    logger.info(f"LOG_CURL: {LOG_CURL}")

    id_file = "/home/Python/OutBoxTrigger/ITS/id_web.txt"

except Exception as e:
    logging.error(f"Error: {e}")


def load_id_web():
    try:
        if os.path.exists(id_file):
            with open(id_file, "r") as file:
                number = int(file.read())
                return number #int(file.read)
        return 0

    except Exception as e:
        logging.error(f"Error: {e}")
        return 0

   
id_load_ =  load_id_web()

def save_id_web(id_web):
    try:
        with open(id_file,"w") as file:
            file.write(str(id_web))

    except Exception as e:
        logging.error(f"Error: {e}")
    

def generate_id_web():  
    global id_load_
    try:
        id_load_ = id_load_ + 1
        save_id_web(id_load_)
        date = datetime.now().strftime("%y%m%d")
        id_result = date + str(id_load_)
        logger.info(f"Generated ticket ID: {id_result}")
        return id_result
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None
   

@basic_auth.verify_password
def verify_password(username, password):
    try:
        if username == AUTH_CREDENTIAL['USERNAME'] and password == AUTH_CREDENTIAL['PASSWORD']:
            return username
        return None

    except Exception as e:
        logging.error(f"Error: {e}")
        return None
    

# Log the request path and body before each request
@app.before_request
def before_request():
    try:
        request_json = request.get_json(silent=True)
        logger.info(f"Route: {request.path} \n Request body: {request_json}")

    except Exception as e:
        logging.error(f"Error: {e}")


# Handle invalid credentials or missing credentials
@basic_auth.error_handler
def basic_auth_error_handler():
    try:
        logger.error("Invalid or missing Basic Authentication credentials")
        return jsonify({"message": "Invalid or missing Basic Authentication credentials", "code": "2"}), 401
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None
  

# Handle expired tokens
@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    try:
        logger.error("The token has expired")
        return jsonify({"message": "The token has expired", "code": "2"}), 401
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


# Handle invalid tokens
@jwt.invalid_token_loader
def invalid_token_callback(error):
    try:
        logger.error("The token is invalid")
        return jsonify({"message": "The token is invalid", "code": "2"}), 401

    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None
    

# Handle missing tokens
@jwt.unauthorized_loader
def missing_token_callback(error):
    try:
        logger.error("Token is missing")
        return jsonify({"message": "Token is missing", "code": "2"}), 401
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


def create_connection_to_rabbitmq():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                RABBITMQ_CONFIG["HOST"],
                RABBITMQ_CONFIG["PORT"],
                credentials=pika.PlainCredentials(RABBITMQ_CONFIG["USERNAME"], RABBITMQ_CONFIG["PASSWORD"])))
        channel = connection.channel()
        return connection, channel
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


def send_message_to_rabbitmq(message):
    try:
        connection, channel = create_connection_to_rabbitmq()
        channel.queue_declare(queue=RABBITMQ_CONFIG["QUEUE_NAME"], durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_CONFIG["QUEUE_NAME"],
            body=message)

        logger.info(f"Message sent to RabbitMQ: {message[:255] + '...' if len(message) > 255 else message}")

    except Exception as e:
        logging.error(f"Error: {e}")

    finally:
        connection.close()


def parse_uri(uri):
    try:
        parsed_uri = urlparse(uri)
        host = parsed_uri.netloc  # Lấy host (bao gồm cả domain và cổng nếu có)
        # host = parsed_uri.hostname # Lấy domain (không lấy cổng)
        path = parsed_uri.path  # Lấy đường dẫn
        scheme = parsed_uri.scheme  # Lấy scheme (http hoặc https)

        return host, path, scheme
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None, None


def get_jwt_token():
    global cached_token, token_timestamp

    # Check if token is cached and not expired
    if cached_token and token_timestamp and datetime.now() - token_timestamp < timedelta(minutes=5):
        logger.info("Returning cached JWT token.")
        return cached_token

    request_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_send_message = False
    headers = {
        "Content-Type": "application/json",
        "Cookie": "BIGipServerdr_api_gw_dev_8000_pool=1635656714.16415.0000",
    }
    parsed_url = urlparse(TOKEN_CREDENTIAL["URL"])

    message = {
        "Host": parsed_url.hostname,
        "Path": parsed_url.path,
        "RequestBody": json.dumps({"username": TOKEN_CREDENTIAL["USERNAME"], "password": TOKEN_CREDENTIAL["PASSWORD"]}),
        "RequestHeader": json.dumps(headers),
        "RequestDate": request_date,
        "ResponseDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        response = requests.post(
            TOKEN_CREDENTIAL["URL"],
            json={"username": TOKEN_CREDENTIAL["USERNAME"], "password": TOKEN_CREDENTIAL["PASSWORD"]},
            headers=headers,
            # proxies=proxies,
        )

        message["Response"] = response.text
        message["StatusCode"] = response.status_code
        send_message_to_rabbitmq(json.dumps(message))
        is_send_message = True

        if response.status_code == 200:
            logger.info("JWT token retrieved successfully")
            cached_token = response.json().get("token")
            token_timestamp = datetime.now()
            return cached_token
        else:
            logger.error(
                f"Failed to get JWT token: {response.status_code} {response.text}"
            )
            return None
    except Exception as e:
        if (is_send_message == False):
            message["Response"] = str(e)
            send_message_to_rabbitmq(json.dumps(message))
        logger.error(f"Error getting JWT token: {e}")
        raise e


def insert_khachhang_and_get_id(name, phone_number, email, address, company, group):
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connecting to MySQL")
            return None

        select_query = """
            SELECT id, c_tenDN
            FROM jwdb.app_fd_sp_khachhang
            WHERE MATCH(c_all_phones) AGAINST (%s IN BOOLEAN MODE)
            """
        cursor_select = conn.cursor()
        cursor_select.execute(select_query, (phone_number,))
        all_data = cursor_select.fetchall()

        # Get the first result (if available)
        data = all_data[0] if all_data else None
        cursor_select.close()  # Close cursor after fetching

        if data:
            return data[0]
        else:
            cursor_insert = conn.cursor()
            insert_query = """
                        INSERT INTO jwdb.app_fd_sp_khachhang
                        (id, c_tenDN, c_sdt_cif, c_email, c_diaChi,c_companyName, dateCreated, dateModified,
                        c_individual, c_nhom_kh)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
            date_handle = datetime.now()
            idkh = generate_uuid()
            cursor_insert.execute(
                insert_query,
                (idkh, name, phone_number, email, address, company, date_handle, date_handle, group, group,),
            )
            conn.commit()
            cursor_insert.close()
            conn.close()

            return idkh
    except Exception as e:
        logger.error(f"Error inserting khachhang: {e}, {phone_number}", exc_info=True)
        return None


def getmnemonic(company_code):
    try:
        conn = py_common.connect_to_database(read=True)
        if conn is None:
            logger.error("Error connecting to MySQL")
            return None

        select_query = """
            SELECT mnemonic
            FROM jwdb.app_fd_etl_company
            WHERE company_code = %s
            """
        cursor = conn.cursor()
        cursor.execute(select_query, (company_code,))
        data = cursor.fetchone()
        cursor.close()
        conn.close()

        return data[0] if data else None
    except Exception as e:
        logger.error(f"Error getting mnemonic: {e}")
        return None


def save_ticket(id, name, email, phone_number, address, company, group, l1, l2, l3, l4, l5, description, ticket_id):
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connecting to MySQL")
            return False

        idkh = "" # insert_khachhang_and_get_id(name, phone_number, email, address, company, group)

        mnemonic = getmnemonic(company)
        logger.info(f"IDKH: {idkh}")
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO jwdb.outbox_ticket_websites
            (ticket_id, dateCreated,
            c_tenDN,
            c_email,
            c_Phone,
            c_diaChi,
            c_DonViGan,
            c_individual, -- (c_nhom_kh)
            c_phanLoai,
            c_NhomYeuCau,
            c_DanhMucYeuCau,
            c_ChiTietYeuCau,
            c_level5,
            c_content,
            c_trangThai,
            c_MaTicket,
            c_source,
            c_MucDo,
            c_PhanHangKH, c_CapDoXuLy, dateModified, c_fkKH,c_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
        date_handle = datetime.now()
        cursor.execute(
            insert_query,
            (
                id, date_handle, name, email, phone_number, address,
                company + " - " + mnemonic if company and mnemonic else company,
                group,
                l1, l2, l3, l4, l5,
                description, 'Mới', ticket_id, 'Website', 'Trung bình', 'Mass', 'Xử lý lần đầu', date_handle,
                idkh,'NEW'
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving ticket: {e}", exc_info=True)
        return False


def update_request_id(ticket_id, request_id):
    """
    Update the request ID for a ticket in the database.
    Args:
        ticket_id (str): The ticket ID.
        request_id (str): The request ID.
    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connecting to MySQL")
            return False

        cursor = conn.cursor()
        update_query = """
        UPDATE jwdb.app_fd_sp_tickets
        SET c_refNo = %s,
        c_field2 = '1'
        WHERE id = %s
        """
        cursor.execute(update_query, (request_id, ticket_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error updating request_id: {e}")
        return False


def update_ticket_status(ticket_id, status="Completed"):
    """
        Update the status of a ticket in the database.
        Args:
            ticket_id (str): The ticket ID.
            status (str): The new status. Default is "Completed".
        Returns:
            bool: True if the update was successful, False otherwise.
    """
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connecting to MySQL")
            return False

        cursor = conn.cursor()
        update_query = """
        UPDATE outbox_sp_tickets_tmp
        SET status = %s
        WHERE idtmp = %s
        """
        cursor.execute(update_query, (status, ticket_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error updating status: {e}")
        return False


def check_auth(authorization_header):
    """
        Check the authorization header for valid credentials.
        Args:
            authorization_header (str): The authorization header.
        Returns:
            bool: True if the credentials are valid, False otherwise.
    """
    try:
        encoded_username_password = authorization_header.split("Basic ")[1].strip()
        decoded_username_password = base64.b64decode(encoded_username_password).decode("utf-8")
        username, password = decoded_username_password.split(":")
        if username == AUTH_CREDENTIAL["USERNAME"] and password == AUTH_CREDENTIAL["PASSWORD"]:
            return True
        else:
            return False

    except Exception as e:
        logger.error(f"Error check_auth: {e}")
        return False


def generate_uuid():
    try:
        return str(uuid.uuid4())
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None


def handle_update_its(its_id, data):
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connect Mysql")
            return False

        update_query = f"""
        UPDATE jwdb.app_fd_sp_tickets
        SET c_huongxl = %s,
            modifiedByName = %s,
            dateModified = %s,
            c_fileUpload_xl = %s,
            c_trangThai = 'Đã xử lý'
        WHERE c_refNo = %s
        """

        attachment_files = []
        for file in data.get("attachFiles", []):
            host, path, scheme = parse_uri(file)
            attachment_files.append(f"{ITS_HOST_IMAGE}{path}")

        cursor_update = conn.cursor()
        cursor_update.execute("SET SQL_SAFE_UPDATES = 0;")
        cursor_update.execute(
            update_query,
            (
                clean_and_add_domain_to_src(data.get("itsContent"), ITS_HOST_IMAGE),
                data.get("pic"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ",".join(attachment_files),
                its_id,
            ),
        )
        cursor_update.execute("SET SQL_SAFE_UPDATES = 1;")
        conn.commit()
        conn.close()
        cursor_update.close()

        return True

    except Exception as e:
        logger.error(f"Error handle_update_its: {e}")
        return False


def read_json_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        logger.error(f"File '{file_path}' không tồn tại.")
        return None
    except json.JSONDecodeError:
        logger.error(f"File '{file_path}' không phải là file JSON hợp lệ.")
        return None


def create_message(
        host=None, path=None, request_date=None, response_date=None, request_body=None, request_header=None,
        response=None, status=None):
    try:
        message = {
            "Host": host,
            "Path": path,
            "RequestBody": json.dumps(request_body, ensure_ascii=False) if request_body else None,
            "RequestHeader": json.dumps(request_header) if request_header else None,
            "RequestDate": request_date if request_date else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ResponseDate": response_date if response_date else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "StatusCode": status,
            "Response": json.dumps(response) if response else None,
        }
        return message
    except Exception as e:
        logger.error(f"Create message - Error: {e}", exc_info=True)
        raise e


def send_api_create_ticket(
        token, subject, description, requester_id, phone_number, template_id, cat_id, sub_cat_id,
        item_id, attachments, complain_id, login_name, path):

    headers = {
        "Accept": "application/json; charset=utf-8",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    data = {
        "subject": subject,
        "requester_id": requester_id,
        "phone_number": phone_number,
        "template_id": template_id,
        "cat_id": cat_id,
        "sub_cat_id": sub_cat_id,
        "item_id": item_id,
        "attachments": attachments,
        "complain_id": complain_id,
        "login_name": login_name,
        "description": description,
    }
    message = create_message(
        host=API_HOST,
        path=path,
        request_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        request_body=data,
        request_header=headers, )
    try:
        json_data = json.dumps(data, ensure_ascii=False)
        url = API_HOST + path

        response = requests.post(url, headers=headers, proxies=proxies, data=json_data, )
        message["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message["StatusCode"] = response.status_code
        message["Response"] = response.text
        send_message_to_rabbitmq(json.dumps(message, ensure_ascii=False))

        if LOG_CURL == True:
            log_curl(url, "POST", headers, json_data)
        return response

    except Exception as e:
        logger.error(f"Send API create ticket - Error: {e}")
        raise e


def send_api_update_ticket(token, description, phone_number, attachments, path):

    headers = {
        "Accept": "application/json; charset=utf-8",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    data = {
        "phone_number": phone_number,
        "attachments": attachments,
        "description": description,
    }
    message = create_message(
        host=API_HOST,
        path=path,
        request_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        request_body=data,
        request_header=headers, )
    try:
        json_data = json.dumps(data, ensure_ascii=False)
        url = API_HOST + path

        response = requests.put(url, headers=headers, proxies=proxies, data=json_data, )
        message["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message["StatusCode"] = response.status_code
        message["Response"] = response.text
        send_message_to_rabbitmq(json.dumps(message, ensure_ascii=False))

        if LOG_CURL == True:
            log_curl(url, "PUT", headers, json_data)
        return response

    except Exception as e:
        logger.error(f"Send API  update ticket - Error: {e}")
        raise e


def send_api_get_info(route, path):
    token = get_jwt_token()
    if not token:
        logger.error(f"API: {route} - Error: No token available.")
        return jsonify({"error": "Failed to get token"}), 500

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    url = API_HOST + path
    message = create_message(
        host=API_HOST,
        path=path,
        request_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        request_body=None,
        request_header=headers, )
    try:
        if LOG_CURL == True:
            log_curl(url, "GET", headers, None)

        response = requests.get(url, headers=headers, proxies=proxies)

        message["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message["StatusCode"] = response.status_code
        message["Response"] = response.text
        send_message_to_rabbitmq(json.dumps(message, ensure_ascii=False))

        content = response.json()
        if content.get("res_code", {}).get("error_code") == "00":
            data = content.get("data", {}).get("details", [])
            return jsonify({"code": "0", "message": "Success", "data": data}), 200

        logger.error(f"API: {route} - Error: {content}")
        return (jsonify(
            {
                "message": content.get("res_code", {}).get("error_desc") if content.get("res_code") else
                (content.get("message") if content.get("message") else str(response.content)),
                "url": url,
                "data": None,
                "code": "1"
            }), 400 if content and content.get("res_code") and content.get("res_code").get("error_code") != '99'
                else 500)
    except Exception as e:
        logger.error(f"API: {route} - Error: {e}", exc_info=True)
        return jsonify({"code": "2", "message": f"{str(e)}", "data": None}), 500


def get_data(route, path):
    token = get_jwt_token()
    if not token:
        logger.error(f"API: {route} - Error: No token available.")
        return jsonify({"error": "Failed to get token"}), 500

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    url = API_HOST + path
    message = create_message(
        host=API_HOST,
        path=path,
        request_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        request_body=None,
        request_header=headers, )
    try:
        if LOG_CURL == True:
            log_curl(url, "GET", headers, None)

        response = requests.get(url, headers=headers, proxies=proxies)

        message["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message["StatusCode"] = response.status_code
        message["Response"] = response.text
        send_message_to_rabbitmq(json.dumps(message, ensure_ascii=False))

        content = response.json()
        if content.get("res_code", {}).get("error_code") == "00":
            data = content.get("data", {}).get("details", [])
            return data

        return []
    except Exception as e:
        logger.error(f"API: {route} - Error: {e}", exc_info=True)
        return []


def log_curl(url, method, headers, json_data):
    try:
        header_str = " ".join([f'-H "{k}: {v}"' for k, v in headers.items()])
        curl = f'curl -X {method} "{url}" ' \
            f'{header_str} '

        if json_data:
            curl += f'-d \'{json_data}\''

        logger.info(f"[+] cURL: {curl}")

    except Exception as e:
        logging.error(f"Error: {e}")


# route for logging user in
@app.route('/api/v1/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            logger.error("Missing username or password")
            return jsonify({"message": "Missing username or password", "code": "1"}), 400

        if username == AUTH_CREDENTIAL['USERNAME'] and password == AUTH_CREDENTIAL['PASSWORD']:
            # Generate a token
            access_token = create_access_token(identity=username)
            decoded_token = decode_token(access_token)
            exp_time = datetime.fromtimestamp(decoded_token['exp']).strftime('%Y-%m-%d %H:%M:%S')

            logger.info(f"User {username} logged in successfully")
            return jsonify(access_token=access_token, expires_at=exp_time, message="Success", code="0"), 200
        else:
            logger.error("Invalid credentials")
            return jsonify({"message": "Invalid credentials", "code": "1"}), 400
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None
    

@app.route("/health", methods=["GET"])
def health_check():
    try:
        logger.info(f"Route: {request.path} ")
        return jsonify({"status": "UP"}), 200
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


def clean_and_add_domain_to_src(html, domain):
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Lặp qua tất cả các thẻ img
        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                # Loại bỏ ký tự &quot; ở đầu và cuối src nếu có
                src = re.sub(r'^&quot;|&quot;$', '', src)

                # Kiểm tra nếu src là đường dẫn tương đối, nếu đúng thì thêm domain vào
                if not src.startswith(('http://', 'https://')):
                    img['src'] = domain + src if src.startswith('/') else domain + '/' + src

        return str(soup)

    except Exception as e:
        logging.error(f"Error: {e}")
        return None


@app.route("/lv24h-callcenter-web/api/its/<int:itsId>", methods=["PUT"])
def endpoint_update_its(itsId):
    """
        Update an ITS record via a PUT request.
        Args:
            its_id (int): The ITS ID.
        Returns:
            Response: The response object.
    """
    try:
        data = request.json

        request_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data_log = create_message(
            "",
            f"/lv24h-callcenter-web/api/its/{itsId}",
            request_date,
            request_date,
            data,
            None,
            None,
            "", )

        if itsId and data:
            conn_select = py_common.connect_to_database(read=False)

            select_query = f"""
                SELECT c_refNo, c_MaTicket
                FROM jwdb.app_fd_sp_tickets
                WHERE c_refNo = %s
                """
            cursor_select = conn_select.cursor()
            cursor_select.execute(select_query, (itsId,))
            data1 = cursor_select.fetchone()
            logger.info(f"Route: {request.path} - search:\n {cursor_select.statement}")
            logger.info(f"Route: {request.path} - data fetched: {data1}")

            if data1 is None:
                logger.info(f"No record found with c_refNo = {itsId}. Update aborted.")
                conn_select.close()
                cursor_select.close()

                response = {"message": "No record found", "code": "1"}
                data_log["StatusCode"] = "404"
                data_log["Response"] = json.dumps(response)
                data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                send_message_to_rabbitmq(json.dumps(data_log))

                return jsonify(response), 404

            conn_select.close()
            cursor_select.close()

            success = handle_update_its(itsId, data)

            if success:
                response = {"message": "Success", "code": "0"}
                data_log["StatusCode"] = "200"
                data_log["Response"] = json.dumps(response)
                data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                send_message_to_rabbitmq(json.dumps(data_log))

                return jsonify(response), 200
            else:
                response = {"message": "Failed to update record", "code": "1"}
                data_log["StatusCode"] = "500"
                data_log["Response"] = json.dumps(response)
                data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return jsonify(response, code="1"), 500

        else:
            response = {"message": "Failed to update record", "code": "1"}
            data_log["StatusCode"] = "200"
            data_log["Response"] = json.dumps(response)
            data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return jsonify(message="Invalid itsId or data", code="1"), 400
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None
    

def get_ticket_id_outbox(id):
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logging.error("Error connecting to MySQL")
            return None

        select_query = """
            SELECT ticket_id, c_status, c_MaTicket
            FROM jwdb.outbox_ticket_websites
            WHERE ticket_id = %s AND c_status = 'DONE' AND c_MaTicket is not null AND c_MaTicket !=''
            """
        cursor_select = conn.cursor()
        cursor_select.execute(select_query, (id,))
        data_ticket = cursor_select.fetchone()
        cursor_select.close()  # Close cursor after fetching

        if data_ticket:
            ma_ticket = data_ticket[2]  # Cột thứ 2 trong kết quả trả về là c_MaTicket
            return ma_ticket
        else:
            return None

    except Exception as e:
        logging.error(f"Error search ticket: {e}, {id}", exc_info=True)
        return None


def fetch_ma_ticket(id):
    """
    Lặp lại quá trình lấy ma_ticket cho đến khi có giá trị hợp lệ.
    """
    ma_ticket = None

    try:
        # Lặp cho đến khi có ma_ticket hợp lệ
        while ma_ticket is None:
            logger.info("Đang lấy ma_ticket...")
            ma_ticket = get_ticket_id_outbox(id)

            if ma_ticket is None:
                logger.info("Không có ma_ticket, thử lại sau...")
                time.sleep(2)  # Đợi 2 giây rồi thử lại

        logger.info(f"Đã lấy được ma_ticket: {id}")
        return ma_ticket
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None

    
def generate_ticket():
    try:
        conn = py_common.connect_to_database(read=False)
        if not conn:
            logging.error("Database connection failed.")
            return None

        cursor = conn.cursor()
        cursor.execute("SELECT msb_api.counter_maticket()")
        result = cursor.fetchone()
        cursor.fetchall()
        conn.commit()
        conn.close()

        if result and result[0]:
            logging.info(f"Generated ticket ID: {result[0]}")
            return result[0]
        else:
            logging.error("Failed to generate ticket from database.")
            return None
    except Exception as e:
        logging.error(f"Error generating ticket: {e}")
        return None


@app.route("/api/v1/its/create-ticket-website", methods=["POST"])
@jwt_required()
def endpoint_create_tickets_website():
    request_data = request.json
    request_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_log = create_message(
        "",
        request.path,
        request_date,
        request_date,
        request_data,
        None,
        None,
        "", )
    try:
        name = request_data.get("name")
        email = request_data.get("email")
        phone_number = request_data.get("phoneNumber")
        address = request_data.get("address")
        company = request_data.get("company")
        group = request_data.get("group")
        l1 = request_data.get("l1")
        l2 = request_data.get("l2")
        l3 = request_data.get("l3")
        l4 = request_data.get("l4")
        l5 = request_data.get("l5")
        description = request_data.get("description")

        invalid_fields = []
        if not name:
            invalid_fields.append("name")
        if not phone_number:
            invalid_fields.append("phoneNumber")
        if not l1:
            invalid_fields.append("l1")
        if not l2:
            invalid_fields.append("l2")
        if not l3:
            invalid_fields.append("l3")

        if invalid_fields:
            logger.error(f"Invalid fields: {invalid_fields}")
            # return jsonify(data=None, code="1", message=f"Missing required fields: {invalid_fields}"), 400

            response = {"data": None, "code": "1", "message": f"Missing required fields: {invalid_fields}"}
            data_log["StatusCode"] = "400"
            data_log["Response"] = json.dumps(response)
            data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            send_message_to_rabbitmq(json.dumps(data_log))

            return jsonify(response), 400

        product_name = ''
        amount = ''
        if l1 == 'Tư vấn':
            product_name = l3
            l3 = ''
        if l1 == 'Đặt lịch hẹn' and l2 != 'Tư vấn sản phẩm':
            amount = l3
            l3 = ''

        conn = py_common.connect_to_database(read=False)
        cursor = conn.cursor()
        query = """
           SELECT c_nhom as nhom, c_l1 as l1, c_l2 as l2, c_l3 as l3
            , c_l1_CRM as l1_crm , c_l2_CRM as l2_crm, c_l3_CRM as l3_crm, c_l4_CRM as l4_crm
            FROM jwdb.app_fd_website_mapping
           """
        df = pd.read_sql(query, conn)
            
        conn.close()
        cursor.close()

        condition = (
                (df["nhom"] == group)
                & (df["l1"] == l1)
                & (df["l2"] == l2)
                & (df["l3"] == l3)
        )

        result = df[condition]
        
        if result.empty:
            logging.error(f"No matching data found for {group},{l1},{l2},{l3},{l4}")
            response = {"code": "1", "message": f"No matching data found for {group},{l1},{l2},{l3},{l4}"}
            data_log["StatusCode"] = "400"
            data_log["Response"] = json.dumps(response)
            data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            send_message_to_rabbitmq(json.dumps(data_log))

            return jsonify(response), 400
            # return jsonify({"code": "1", "message": f"No matching data found for {group},{l1},{l2},{l3},{l4}"}), 400
        
        list_id = result.iloc[0].to_dict()
        logging.info(f"Data matching: {group},{l1},{l2},{l3},{l4}")
        l1_id = list_id.get('l1_crm', "")
        l2_id = list_id.get('l2_crm', "")
        l3_id = list_id.get('l3_crm', "")
        l4_id = list_id.get('l4_crm', "")
        
        logger.info(f'ID level: {list_id}')

        if l1 == 'Tư vấn' or (l1 == 'Đặt lịch hẹn' and l2 != 'Tư vấn sản phẩm'):
            description = f"{product_name} - {description}"

        if l1 == 'Đặt lịch hẹn':
            if l2 == 'Tư vấn sản phẩm':
                # l4 =l5
                l4 = ''
            else:
                l4 = amount
        
        id = generate_uuid()
        #ticket_id = generate_ticket() # generate_id_web()
        ticket_id =""
        success = save_ticket(
            id, name, email, phone_number, address, company, group,
            l1_id, l2_id, l3_id, l4 if l4_id else l4_id, l5, description, ticket_id)
            
        ticket_id = fetch_ma_ticket(id)
        logger.info(f"Generated ticket ID: {id} - {ticket_id}")
        if success:
            # return jsonify({"code": "0", "message": "Success", "data": {"ticketId": ticket_id}}), 200
            response = {"code": "0", "message": "Success", "data": {"ticketId": ticket_id}}
            data_log["StatusCode"] = "200"
            data_log["Response"] = json.dumps(response)
            data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            send_message_to_rabbitmq(json.dumps(data_log))

            return jsonify(response), 200
        else:
            # return jsonify({"code": "0", "message": "Success", "data": {"ticketId": ticket_id}}), 200
            response = {"code": "1", "message": "Failed to save ticket"}
            data_log["StatusCode"] = "500"
            data_log["Response"] = json.dumps(response)
            data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            send_message_to_rabbitmq(json.dumps(data_log))

            return jsonify(response), 500
            # return jsonify({"code": "1", "message": "Failed to save ticket"}), 500

    except Exception as e:
        logger.error(f"Create ticket fail: {str(e)}", exc_info=True)
        response = {"code": "1", "message": "An error occurred while processing your request"}
        data_log["StatusCode"] = "500"
        data_log["Response"] = json.dumps(response)
        data_log["ResponseDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        send_message_to_rabbitmq(json.dumps(data_log))

        return jsonify(response), 500
        # return jsonify({"code": "1", "message": "An error occurred while processing your request"}), 500


@app.route("/api/v1/its/get-ticket-status", methods=["POST"])
@jwt_required()
def endpoint_get_ticket_status():
    request_data = request.json

    try:
        ticket_id = request_data.get("ticketId")
        phone_number = request_data.get("phoneNumber")

        invalid_fields = []
        if not ticket_id:
            invalid_fields.append("ticketId")
        if not phone_number:
            invalid_fields.append("phoneNumber")

        if invalid_fields:
            logger.error(f"Invalid fields: {invalid_fields}")
            return jsonify(data=None, code="1", message=f"Missing required fields: {invalid_fields}"), 400

        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connect Mysql")
            return False

        select_query = f"""
            SELECT c_MaTicket, c_trangThai, c_huongxl, c_Phone
            FROM jwdb.app_fd_sp_tickets
            WHERE c_MaTicket = %s AND c_Phone = %s
        """
        cursor = conn.cursor()
        cursor.execute(select_query, (ticket_id, phone_number))
        data = cursor.fetchone()
        conn.close()
        cursor.close()

        if data:
            res = {
                'ticketId': data[0],
                'status': data[1],
                'handlerResult': data[2],
                'phoneNumber': data[3]
            }
            return jsonify({"code": "0", "message": "Success", "data": res}), 200
        else:
            return jsonify({"code": "1", "message": "Ticket not found", "data": None}), 404

    except Exception as e:
        logger.error(f"Exception occurred while creating tickets: {str(e)}")
        return jsonify({"code": "1", "message": "An error occurred while processing your request"}), 500


@app.route("/api/v1/its/get-service-category", methods=["POST"])
@basic_auth.login_required
def endpoint_get_service_categories():
    try:
        type_service = request.json.get("type", "ITS")

        path = API_PATH["API_ITS"]["GET_SERVICE_CATEGORY"] if type_service == "ITS" else API_PATH["API_HO"][
            "GET_SERVICE_CATEGORY"]

        if ENV == "DEV":
            data = read_json_data(SETTINGS["PATH_DATA"])
            if not data or not data.get("service-category"):
                return jsonify({"code": "0", "message": "Success", "data": []}), 200

            return jsonify({"code": "0", "message": "Success", "data": data.get("service-category")}), 200

        return send_api_get_info(request.path, path)
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


@app.route("/api/v1/its/get-template-by-service-cate", methods=["POST"])
@basic_auth.login_required
def endpoint_get_template_by_service_cate():
    try:
        type_service = request.json.get("type", "ITS")

        path = API_PATH["API_ITS"]["GET_TEMPLATE_BY_SERVICE_CATE"] if type_service == "ITS" else API_PATH["API_HO"][
            "GET_TEMPLATE_BY_SERVICE_CATE"]

        if ENV == "DEV":
            data = read_json_data(SETTINGS["PATH_DATA"])
            if not data or not data.get("service-template"):
                return jsonify({"code": "0", "message": "Success", "data": []}), 200

            return jsonify({"code": "0", "message": "Success", "data": data["service-template"]}), 200

        return send_api_get_info(request.path, path)
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


@app.route("/api/v1/its/get-template", methods=["POST"])
@basic_auth.login_required
def endpoint_get_templates():
    try:
        type_service = request.json.get("type", "ITS")

        path = API_PATH["API_ITS"]["GET_TEMPLATE"] if type_service == "ITS" else API_PATH["API_HO"]["GET_TEMPLATE"]

        if ENV == "DEV":
            data = read_json_data(SETTINGS["PATH_DATA"])
            if not data or not data.get("template"):
                return jsonify({"code": "0", "message": "Success", "data": []}), 200

            return jsonify({"code": "0", "message": "Success", "data": data.get("template")}), 200

        return send_api_get_info(request.path, path)

    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


@app.route("/api/v1/its/get-category", methods=["POST"])
@basic_auth.login_required
def endpoint_get_categories():
    try:
        type_service = request.json.get("type", "ITS")

        path = API_PATH["API_ITS"]["GET_CATEGORY"] if type_service == "ITS" else API_PATH["API_HO"]["GET_CATEGORY"]
        # DEV environment
        if ENV == "DEV":
            data = read_json_data(SETTINGS["PATH_DATA"])
            if not data or not data.get("category"):
                return jsonify({"code": "0", "message": "Success", "data": []}), 200

            for category in data.get("category"):
                category.pop("SUB_CATS", None)

            return jsonify({"code": "0", "message": "Success", "data": data.get("category")}), 200

        return send_api_get_info(request.path, path)

    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


@app.route("/api/v1/its/get-sub-category", methods=["POST"])
@basic_auth.login_required
def endpoint_get_sub_categories():
    try:
        type_service = request.json.get("type", "ITS")

        path = API_PATH["API_ITS"]["GET_SUB_CATEGORY"] if type_service == "ITS" else API_PATH["API_HO"]["GET_SUB_CATEGORY"]

        category_id = request.json.get("categoryId")
        if not category_id:
            logger.error(f"API: {request.path} - Error: Missing categoryId")
            return jsonify({"error": "Missing categoryId"}), 400

        # DEV environment
        if ENV == "DEV":
            data = read_json_data(SETTINGS["PATH_DATA"])
            category_id_int = int(category_id)
            if not data or not data.get("category"):
                return jsonify({"code": "0", "message": "Success", "data": []}), 200

            for category in data.get("category"):
                if category.get("ID") == category_id_int:
                    if category.get("SUB_CATS"):
                        for sub_category in category.get("SUB_CATS"):
                            sub_category.pop("ITEMS", None)
                        return jsonify(
                            {
                                "code": "0",
                                "message": "Success",
                                "data": category.get("SUB_CATS")
                            }), 200
                    else:
                        return jsonify({"code": "0", "message": "Success", "data": []}), 200

            return jsonify({"code": "0", "message": "Success", "data": []}), 200

        return send_api_get_info(request.path, path + "/" + category_id)
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None


@app.route("/api/v1/its/get-item", methods=["POST"])
@basic_auth.login_required
def endpoint_get_items():
    try:
        type_service = request.json.get("type", "ITS")

        path = API_PATH["API_ITS"]["GET_ITEM"] if type_service == "ITS" else API_PATH["API_HO"]["GET_ITEM"]

        sub_category_id = request.json.get("subCategoryId")
        if not sub_category_id:
            logger.error(f"API: {request.path} - Error: Missing subCategoryId")
            return jsonify({"error": "Missing subCategoryId"}), 400

        # DEV environment
        if ENV == "DEV":
            data = read_json_data(SETTINGS["PATH_DATA"])
            sub_category_id_int = int(sub_category_id)
            if not data or not data.get("category"):
                return jsonify({"code": "0", "message": "Success", "data": []}), 200

            for category in data.get("category"):
                sub_categories = category.get("SUB_CATS")
                if not sub_categories:
                    continue

                for sub_category in sub_categories:
                    if sub_category.get("ID") == sub_category_id_int:
                        return jsonify(
                            {
                                "code": "0",
                                "message": "Success",
                                "data": sub_category.get("ITEMS") if sub_category.get("ITEMS") else []
                            }), 200
            return jsonify({"code": "0", "message": "Success", "data": []}), 200

        return send_api_get_info(request.path, path + "/" + sub_category_id)

    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None

def process_api_create_ticket(path, request):
    token = get_jwt_token()
    if token is None:
        logger.error(f"API: {request.path} - Error: No token available.")
        return jsonify({"error": "Failed to get token"}), 500

    subject = request.json.get("subject")
    description = request.json.get("description")
    requester_id = request.json.get("requesterId")
    phone_number = request.json.get("phoneNumber")
    template_id = request.json.get("templateId")
    cat_id = request.json.get("categoryId")
    sub_cat_id = request.json.get("subCategoryId")
    item_id = request.json.get("itemId")
    attachments = request.json.get("attachments")
    complain_id = request.json.get("complainId")
    login_name = request.json.get("loginName")

    if not (subject and description and template_id and cat_id and sub_cat_id and item_id):
        logger.error(f"API: {request.path} - Error: Missing required fields")
        return jsonify({"message": "Missing required fields", "code": "1", "data": None}), 400

    try:
        url = API_HOST + path

        response = send_api_create_ticket(
            token, subject, description, requester_id, phone_number, template_id, cat_id, sub_cat_id, item_id,
            attachments, complain_id, login_name, path)

        content = response.json()
        if content.get("res_code", {}).get("error_code") == "00":
            data = content.get("data", {}).get("request", [])
            logger.info(f"API: {request.path} - Success: {data}")
            return jsonify({"code": "0", "message": "Success", "data": data}), 200

        logger.error(f"API: {request.path} - Error >>> : {content}")
        return jsonify(
            {
                "message": content.get("res_code", {}).get("error_desc") if content.get("res_code") else
                (content.get("message") if content.get("message") else str(response.content)),
                "url": url,
                "data": None,
                "code": "1"
            }), 400 if content and content.get("res_code") and content.get("res_code").get(
            "error_code") != "99" else 500
    except Exception as e:
        logger.error(f"API: {request.path} - Error: {e}", exc_info=True)
        return jsonify({"code": "2", "message": "Fail to fetch request", "data": None}), 500


@app.route("/api/v1/its/create-request", methods=["POST"])
@basic_auth.login_required
def endpoint_create_request():
    path = API_PATH["API_ITS"]["CREATE_REQUEST"]
    return process_api_create_ticket(path, request)


@app.route("/api/v1/its/create-request-hos", methods=["POST"])
@basic_auth.login_required
def endpoint_create_request_hos():
    path = API_PATH["API_HO"]["CREATE_REQUEST"]
    return process_api_create_ticket(path, request)


def save_ticket_ho_tmp(ticket_list):
    if not isinstance(ticket_list, list) or not ticket_list:
        logger.error("Invalid ticket list input")
        return False

    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logger.error("Error connecting to MySQL")
            return False

        cursor = conn.cursor()

        insert_query = """
            INSERT INTO jwdb.app_fd_sp_hosupport_tmp
            (id, dateCreated, createdBy, c_nhom_kh, c_cif, c_DanhMucYeuCau,
            c_type_ticket_ho, c_idkh, c_NhomYeuCau, c_ChiTietYeuCau, c_PhanLoai)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Bắt đầu transaction
        conn.start_transaction()
        list_id = []

        for ticket in ticket_list:
            cursor.execute(
                insert_query,
                (
                    ticket.get("id"),
                    datetime.now(),
                    ticket.get("createdBy"),
                    ticket.get("c_nhom_kh"),
                    ticket.get("c_cif"),
                    ticket.get("c_DanhMucYeuCau"),
                    ticket.get("c_type_ticket_ho"),
                    ticket.get("c_idkh"),
                    ticket.get("c_NhomYeuCau"),
                    ticket.get("c_ChiTietYeuCau"),
                    ticket.get("c_PhanLoai"),
                ),
            )
            list_id.append(ticket.get("ticket_id"))

        # Commit transaction nếu không có lỗi
        conn.commit()
        logger.info(f"Successfully saved {len(ticket_list)} tickets. with list id: {list_id}")
        return True, list_id

    except Exception as e:
        conn.rollback()  # Rollback nếu có lỗi
        logger.error(f"Transaction failed, rollback executed: {e}")
        return False

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route("/api/v1/ho/get-info-data", methods=["POST"])
@basic_auth.login_required
def endpoint_get_info_data():
    request_data = request.json

    try:
        nhom_kh = request_data.get("nhom_kh")
        type_ho = request_data.get("type_ho")  # Tài khoản | Thẻ
        type_ticket_ho = request_data.get("type_ticket_ho")  # is level 1 : Tra soát | Rủi ro
        transaction_name = request_data.get("ten_giao_dich")
        card_number = request_data.get("so_the")
        transaction_type = request_data.get("loai_giao_dich")
        merchant_name = request_data.get("merchant_name")

        l1_id, l1, l2_id, l2, l3_id, l3, message = mapping_ho.map_level_ho_auto(
            nhom_kh, type_ticket_ho, transaction_name, card_number, transaction_type,
            merchant_name, type_ho, logger)
        return jsonify(
            {
                "code": "0", "message": message, "data": {
                "l1": l1, "l2": l2, "l3": l3, "l1_id": l1_id, "l2_id": l2_id, "l3_id": l3_id
            }
            }), 200

    except Exception as e:
        logger.error(f"Get info data: {str(e)}", exc_info=True)
        return jsonify({"code": "1", "message": str(e)}), 500


@app.route("/api/v1/ho/get-mail-by-jodcode", methods=["POST"])
@basic_auth.login_required
def get_by_jodcode():
    conn = py_common.connect_to_database(read=False)
    if conn is None:
        logger.error("Error connecting to MySQL")
        return False

    cursor = conn.cursor()
    request_data = request.json
    try:
        ma_ticket = request_data.get("maticket")
        email = get_email_by_jodcode(cursor, ma_ticket, logger)
        logging.info(f"{email}")
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify(
            {
                "message": "success", "data": {
                "email": email
            }
            }), 200


    except Exception as e:
        logger.error(f"Get info data: {str(e)}", exc_info=True)
        return jsonify({"code": "1", "message": str(e)}), 500


@app.route("/api/v1/its/create-ticket-hos-tmp", methods=["POST"])
@basic_auth.login_required
def endpoint_create_ticket_hos_tmp():

    request_data = request.json

    try:
        # Nếu request_data là danh sách, xử lý từng phần tử
        if isinstance(request_data, list):
            processed_data = []

            for item in request_data:
                processed_item = {
                    "id": generate_uuid(),
                    "ticket_id": generate_ticket(),
                    "createdBy": item.get("createdBy"),
                    "c_nhom_kh": item.get("c_nhom_kh"),
                    "c_cif": item.get("c_cif"),
                    "c_DanhMucYeuCau": item.get("c_DanhMucYeuCau"),
                    "c_type_ticket_ho": item.get("c_type_ticket_ho"),
                    "c_idkh": item.get("c_idkh"),
                    "c_NhomYeuCau": item.get("c_NhomYeuCau"),
                    "c_ChiTietYeuCau": item.get("c_ChiTietYeuCau"),
                    "c_PhanLoai": item.get("c_PhanLoai"),
                }
                processed_data.append(processed_item)

            success, list_id = save_ticket_ho_tmp(processed_data)
            if success:
                return jsonify({"code": "0", "message": "Success", "data": {"ticketIds": list_id}}), 200
            else:
                return jsonify({"code": "1", "message": "Failed to save ticket"}), 500


    except Exception as e:
        logger.error(f"Create ticket HO tmp: {str(e)}", exc_info=True)
        return jsonify({"code": "1", "message": str(e)}), 500


def callback(ch, method, properties, body):
    message_str = body.decode()  # Decode byte body to string
    truncated_message = message_str[:255]  # Truncate to 255 characters
    logger.info(f"[>] Received message: {truncated_message}")

    # Assuming the message is a JSON string
    try:
        data = json.loads(body)
        ticket_id = data.get("ticket")
        template_id = data.get("template")
        info = data.get("message")
        type = data.get("type")
        subject = data.get("subject")
        description = data.get("description")
        attachments = data.get("attachments")
        phone_number = data.get("phone_number")

        connection = py_common.connect_to_database(read=False)
        cursor = connection.cursor()
        cursor.execute(
            "SELECT c_refNo, c_MaTicket, c_trangThai FROM jwdb.app_fd_sp_tickets WHERE id = %s",
            (ticket_id,))
        ticket_data = cursor.fetchone()
        cursor.close()
        connection.close()
        logger.info(f"[*] Id ITs: {ticket_data}")

        token = get_jwt_token()
        if token is None:
            raise ValueError("No token available. Exiting callback")

        if ticket_data:
            if ticket_data[0] and ticket_data[2] != "Đã chuyển tiếp":
                logger.warning(f"Ticket ID: {ticket_id} has call create request with ITsId: {ticket_data[0]}.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if ticket_data[0] and ticket_data[2] == "Đã chuyển tiếp":
                logger.warning(f"Ticket ID: {ticket_id} has been forwarded to another department.")

                its_ids = ticket_data[0].split(";")
                # get last id
                its_id = its_ids[-1]
                path = API_PATH["API_ITS"]["UPDATE_REQUEST"] if type == "ITS" else (API_PATH["API_HO"]["UPDATE_REQUEST"]
                                                                                    + its_id)

                response = send_api_update_ticket(
                    token,
                    description,
                    phone_number,
                    attachments,
                    path
                )

                content = response.json()
                if content.get("res_code", {}).get("error_code") == "00":
                    request_id = content.get("data", {}).get("request", {}).get("id")
                    if request_id is None:
                        raise ValueError("No request ID found in response")

                    request_id = ticket_data[0] + ";" + request_id
                    update_request_id(ticket_id, request_id)
                    logger.info(f"Request created successfully: {request_id} - Ticket ID: {ticket_id}")

                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

        requester_id = 13502  # id của CALL CENTER
        login_name = "cskh"  # id của CALL CENTER

        if not info:
            raise ValueError("No info found in message")
        info_id = info.split(";")

        if len(info_id) < 3:
            raise ValueError(f"Invalid message format - {info}")

        category = info_id[0].split("+")
        sub_category = info_id[1].split("+")
        item = info_id[2].split("+")

        sub_code = sub_category[1].split(".")[0]
        template_item = ''
        template_code = ''
        if len(info_id) >= 4:
            template_item = info_id[3]
            template_code = template_item.split(".")[0]

        if template_id == "" and (sub_code or template_code):
            code_for_template = template_code if template_code else sub_code

            if ENV != "DEV":
                response_data = get_data(
                    "/api/v1/its/get-template",
                    API_PATH["API_ITS"]["GET_TEMPLATE_BY_SERVICE_CATE"] if type == "ITS" else API_PATH["API_HO"][
                        "GET_TEMPLATE_BY_SERVICE_CATE"])
                if response_data:
                    for template in response_data:
                        if template["template_name"].startswith(code_for_template):
                            template_id = str(template["template_id"])
                            break
                    logger.info(f"Template searched: {template_id}")
                else:
                    logger.error(f"Empty response data - Get ID From Fixed Code - {code_for_template}")
                    data_template = read_json_data("template.json")

                    # ITs:
                    if type == "ITS":
                        if data_template and data_template.get("ITS"):
                            for template in data_template.get("ITS"):
                                if template.get("template_name").startswith(code_for_template):
                                    template_id = template.get("template_id")
                                    break
                    # HO:
                    else:
                        if data_template and data_template.get("HO"):
                            for template in data_template.get("HO"):
                                if template.get("template_name").startswith(code_for_template):
                                    template_id = template.get("template_id")
                                    break
            else:
                template_id = code_for_template
        # map attachments to ticket_id
        attach_handle = []
        if attachments:
            attachs = attachments.split(";")

            attach_handle = [f"{CRM_HOST_FILE_TICKET}/{ticket_id}/{attach}" for attach in attachs]
        description = description.replace(CRM_TEXT_REPLACE, CRM_HOST_FILE_TICKET)

        logger.info(
            f"Received data: ticket_id = {ticket_id}"
            f"\n\t> template={template_id} - category={category[0]} - sub_cate={sub_category[0]}  - item={item[0]} "
            f"\n\t> subject={subject} "
            f"\n\t> assignments={attach_handle}"
            f"\n\t> description={description[:255] if description else ''}")
        if not ticket_id:
            raise ValueError("No ticket ID found in message")

        if ticket_id == "" or template_id == "" or category[0] == "" or sub_category[0] == "" or item[0] == "":
            raise ValueError("Missing required fields")

        if (ENV == "DEV"):
            logger.info(f"DEV environment, skipping API call.")
            raise ValueError("DEV environment, skipping API call.")

        path = API_PATH["API_ITS"]["CREATE_REQUEST"] if type == "ITS" else API_PATH["API_HO"]["CREATE_REQUEST"]

        if type == "ITS":
            response = send_api_create_ticket(
                token,
                subject,
                description,
                requester_id,
                phone_number,
                template_id=template_id,
                cat_id=category[0],
                sub_cat_id=sub_category[0],
                item_id=item[0],
                attachments=attach_handle,
                login_name=login_name,
                complain_id="",
                path=path)
        else:
            response = send_api_create_ticket(
                token,
                subject,
                description,
                requester_id,
                phone_number,
                template_id=template_id,
                cat_id=category[0],
                sub_cat_id=sub_category[0],
                item_id=item[0],
                attachments=attach_handle,
                login_name=login_name,
                complain_id="",
                path=path)

        content = response.json()
        if content.get("res_code", {}).get("error_code") == "00":
            request_id = content.get("data", {}).get("request", {}).get("id")
            if request_id is None:
                raise ValueError("No request ID found in response")

            update_request_id(ticket_id, request_id)
            logger.info(f"Request created successfully: {request_id} - Ticket ID: {ticket_id}")
        else:
            logger.error(f"Failed to create request: {content}")

    except Exception as e:
        logger.error(e, exc_info=True)

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


MAX_RETRIES = 5  # Maximum retry attempts
RETRY_DELAY = 5  # Delay (in seconds) between retries


def start_consuming():
    config = py_common.read_config(os.path.join(dir_config, "common_config", "config.ini"))
    rabbitmq_config = config['rabbitmq']

    retries = 0
    while retries < MAX_RETRIES:
        try:
            credentials = pika.PlainCredentials(rabbitmq_config['username'], rabbitmq_config['password'])
            connection_params = pika.ConnectionParameters(
                host=rabbitmq_config['host'],
                port=rabbitmq_config['port'],
                credentials=credentials
            )
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            # Declare the queue
            channel.queue_declare(queue=rabbitmq_config['ticketQueue'])

            # Consume messages from the queue
            channel.basic_consume(queue=rabbitmq_config['ticketQueue'], on_message_callback=callback, auto_ack=True)

            logger.info(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()

            retries = 0

        except pika.exceptions.AMQPConnectionError as e:
            retries += 1
            logger.error(f"Connection failed (attempt {retries}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_DELAY)

    logger.critical("Max retries reached. Exiting consumer.")


def main():
    start_consuming()


if __name__ == "__main__":
    Thread(target=main).start()
    app.run(host="0.0.0.0", port=8082)
