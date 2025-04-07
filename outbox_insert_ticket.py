import os
from flask import Flask
import importlib.util
import logging.handlers
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import uuid

import http.client
import json

base_dir = "/home/Python/OutBoxTrigger/ITS"
lib_file_path = "/home/Python/py_common.py"

spec = importlib.util.spec_from_file_location("py_common", lib_file_path)
py_common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(py_common)

log_dir = os.path.join(base_dir, "logs")

if not os.path.exists(log_dir):
        os.makedirs(log_dir)

logging.basicConfig(level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.handlers.TimedRotatingFileHandler(os.path.join(log_dir, "log_outbox.txt"), when="midnight", interval=1, ),
            logging.StreamHandler()
        ])
logging.getLogger(__name__)

app = Flask(__name__)

def generate_uuid():
    return str(uuid.uuid4())

def insert_khachhang_and_get_id(name, phone_number, email, address, company, group):
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logging.error("Error connecting to MySQL")
            return None

        select_query = """
            SELECT id, c_tenDN
            FROM jwdb.app_fd_sp_khachhang
            WHERE MATCH(c_all_phones) AGAINST (%s IN BOOLEAN MODE)
            limit 1
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
        logging.error(f"Error inserting khachhang: {e}, {phone_number}", exc_info=True)
        return None

def get_cif(mobileNumber):
    try:
        conn = http.client.HTTPConnection("10.163.69.49",8085)
        payload = json.dumps({
            "cif":"",
            "mobileNumber": mobileNumber,
            "idkh": ""
        })
        headers = {
            "X-API-Key": "M2N4MjAyNDpMdnBiMTIzNCEjJEA=",
            "Authorization": "Basic M0NYU1lTVEVNOm8xY1lkR1NkUVQ=",
            "Content-Type": "application/json"
        }
        conn.request("POST", "/api/gw/get_customer?type=ivr",  body=payload, headers=headers)
        print(payload)
        print(headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        rs = json.loads(data)
        
        
        if res.status != 200:
            logging.error(f"API call failed: {res.status} - {data}")
            return None,mobileNumber
            
        cif = ''    
        if rs['data']: 
            cif = rs['data'][0]['cif']
            logging.info(cif)
        
        return cif,mobileNumber

    except Exception as e:
        print(f"Error in get_cif: {str(e)}")
        return None,mobileNumber



def get_id_khachhang(cif,phoneNumberCif,name, phone_number, email, address, company, group):
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logging.error("Error connecting to MySQL")
            return None
    
        select_query = """
            SELECT id, c_tenDN
            FROM jwdb.app_fd_sp_khachhang
            WHERE c_cif = %s
            limit 1
            """
        cursor_select = conn.cursor()
        cursor_select.execute(select_query, (cif,))
        all_data = cursor_select.fetchall()

        # Get the first result (if available)
        data = all_data[0] if all_data else None
        cursor_select.close()  # Close cursor after fetching

        if data:
            logging.info(f"Get idKH with id {data[0]}  cif : {cif}")
            return data[0]
        else:
            select_query = """
                SELECT id, c_tenDN
                FROM jwdb.app_fd_sp_khachhang
                WHERE MATCH(c_all_phones) AGAINST (%s IN BOOLEAN MODE)
                limit 1
                """
            cursor_select = conn.cursor()
            cursor_select.execute(select_query, (phoneNumberCif,))
            all_data = cursor_select.fetchall()

            # Get the first result (if available)
            data = all_data[0] if all_data else None
            
            cursor_select.close()  # Close cursor after fetching
            cursor_update = conn.cursor()
            if data:
                logging.info(f"Get idKH with id {data[0]}  phone : {phoneNumberCif}")
                update_query = """
                    UPDATE jwdb.app_fd_sp_khachhang
                    SET c_cif = %s
                    WHERE id = %s
                    """
                cursor_update.execute(update_query,(cif,data[0],))
                conn.commit()
                cursor_update.close()
                conn.close()
                logging.info(f"Update cif with id {data[0]}  cif : {cif}")
                return data[0]
            else:
                cursor_insert = conn.cursor()
                insert_query = """
                            INSERT INTO jwdb.app_fd_sp_khachhang
                            (id, c_tenDN, c_sdt_cif, c_email, c_diaChi,c_companyName, dateCreated, dateModified,
                            c_individual, c_nhom_kh,c_cif)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
                            """
                date_handle = datetime.now()
                idkh = generate_uuid()
                cursor_insert.execute(
                    insert_query,
                    (idkh, name, phone_number, email, address, company, date_handle, date_handle, group, group,cif,),
                )
                conn.commit()
                cursor_insert.close()
                conn.close()
                return idkh
    except Exception as e:
        logging.error(f"Error search khachhang: {e}, {phoneNumberCif}", exc_info=True)
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
     
def save_ticket_db(id, dateCreated, c_tenDN, c_email, c_Phone, c_diaChi, c_DonViGan, c_individual, c_phanLoai, c_NhomYeuCau, c_DanhMucYeuCau, c_ChiTietYeuCau, c_level5, c_content,
        c_trangThai,c_MaTicket,c_source,c_MucDo,c_PhanHangKH,c_CapDoXuLy,dateModified,conn,web_id):
    try:
        cif = get_cif(c_Phone)
        logging.info(f"Cif : {cif}")
        if cif[0]:
            logging.info(f"Geting idKH with cif : {cif[0]}")
            #idkh = get_id_khachhang(cif[0],cif[1],c_tenDN, c_Phone, c_email, c_diaChi, c_DonViGan, c_individual)
            idkh = cif[0]
        else:      
            idkh = insert_khachhang_and_get_id(c_tenDN, c_Phone, c_email, c_diaChi, c_DonViGan, c_individual)
        
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO jwdb.app_fd_sp_tickets
            (id, dateCreated,
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
            c_source,
            c_MucDo,
            c_PhanHangKH, c_CapDoXuLy, dateModified,c_MaTicket, c_fkKH,web_id, c_CIF,createdBy, createdByName, modifiedBy, modifiedByName)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s,%s,%s,%s,%s)
            """
        
        cursor.execute(
            insert_query,
            (
                id, dateCreated, c_tenDN, c_email, c_Phone, c_diaChi,
                c_DonViGan,
                c_individual,
                c_phanLoai, c_NhomYeuCau, c_DanhMucYeuCau, c_ChiTietYeuCau, c_level5,
                c_content, c_trangThai,c_source, c_MucDo, c_PhanHangKH, c_CapDoXuLy, dateModified, c_MaTicket, idkh,web_id,
                cif[0],'Web', 'Web', 'Web', 'Web'
                
            ),#21
        )
        conn.commit()
        update_query = """
            UPDATE jwdb.outbox_ticket_websites
            SET c_status = 'DONE', c_MaTicket =%s
            WHERE ticket_id = %s
        """
        cursor.execute(update_query,(c_MaTicket,id))
        conn.commit()
        logging.info(f"Ticket with id {c_MaTicket}  khachhang:{idkh}")
        return True
    except Exception as e:
        logging.error(f"Error saving ticket: {e}", exc_info=True)
        return False


def fetch_and_store_data():
    try:
        conn = py_common.connect_to_database(read=False)
        if conn is None:
            logging.error("Error connecting to MySQL")
            return False

        cursor = conn.cursor()
        fetch_query = """
            SELECT id,ticket_id,dateCreated, c_tenDN, c_email, c_Phone, c_diaChi, c_DonViGan, c_individual, c_phanLoai, c_NhomYeuCau, c_DanhMucYeuCau,c_ChiTietYeuCau,c_level5,c_content,c_trangThai,c_MaTicket,c_source,c_MucDo,c_PhanHangKH,c_CapDoXuLy,dateModified           
            FROM jwdb.outbox_ticket_websites
            WHERE c_status <> 'DONE' 
            OR c_status is null 
            and dateCreated > DATE_SUB(NOW(), INTERVAL 2 DAY)
            """
        cursor.execute(fetch_query)
        results = cursor.fetchall()
        #20
        if not results:
            logging.info("No data found with c_status <> 'DONE'")
            
        for row in results:
            maticket = generate_ticket()
            success = save_ticket_db(row[1], row[2],row[3],row[4],row[5],row[6],row[7],row[8],
                row[9],row[10], row[11],row[12],row[13],row[14],row[15],maticket,
                row[17],row[18],row[19],row[20],row[21],conn,maticket)
            if success:             
                logging.info(f"Save ticket success with ticket id {maticket}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error saving ticket: {e}", exc_info=True)
        return False
        

        
def start_job():
    logging.info("Job running : ")
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_store_data,'interval',seconds=7)
    scheduler.start()
    
if __name__ == "__main__":
    start_job()
    app.run(debug=False,host='0.0.0.0',port='5109')