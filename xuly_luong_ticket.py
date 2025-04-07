import os
import logging
from logging.handlers import RotatingFileHandler
import uuid
from datetime import datetime
from configparser import ConfigParser
import mysql.connector
import pandas as pd
import json
import time
import fcntl
import pika
from SLA.get_list_email import get_list_email
from SLA.handle_luong_xuly import handle_automatic_ticket, handle_manual_ticket
from SLA.sla import update_followup_processing, update_sla_manual
from SLA.handle_luong_ho_auto import handle_ho_auto

LOCK_FILE = "/tmp/create_luong_ticket.lockfile"
LOCK_TIMEOUT = 600  # Timeout 10 minutes
SLEEP_INTERVAL = 5  # Sleep interval between each check in seconds


# Load config
def read_config(config_path):
    try:
        config = ConfigParser()
        config.read(config_path)
        return config
    
    except FileNotFoundError as e:
        # Xử lý khi tệp cấu hình không tồn tại
        logging.error(f"Lỗi: Không tìm thấy tệp cấu hình: {e}")

    except PermissionError as e:
        # Xử lý khi không có quyền đọc tệp cấu hình
        logging.error(f"Lỗi: Không có quyền đọc tệp cấu hình: {e}")

    except Exception as e:
        # Bắt các lỗi không xác định khác
        logging.error(f"Error: {e}")

def connect_to_database(read=True):
    try:
        config = read_config(
            os.path.join(os.path.dirname(__file__), "common_config", "config.ini")
        )
        db_config = "mysql_slave" if read else "mysql_master"
        connection = mysql.connector.connect(
            host=config[db_config]["host"],
            user=config[db_config]["user"],
            password=config[db_config]["password"],
            database=config[db_config]["database"],
        )
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None


# Hàm thiết lập ghi log
def setup_logging():
    try:
        config = read_config(
            os.path.join(os.path.dirname(__file__), "common_config", "config.ini")
        )
        log_dir = config["logging"]["log_dir_popup"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        log_filepath = os.path.join(log_dir, "Log_Xuly_TK.log")

        # Thiết lập handler để ghi log vào file và tự động quay vòng file khi file đạt dung lượng 20 MB
        handler = RotatingFileHandler(log_filepath, maxBytes=20 * 1024 * 1024, backupCount=4)  # 4 backup files, tổng cộng 5 file log

        # Định dạng log
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s [Log_TK_Call_] - %(message)s"
        )
        handler.setFormatter(formatter)

        # Cài đặt logging
        logging.basicConfig(level=logging.INFO, handlers=[handler])

        # Thiết lập mức độ ghi log cho các thư viện bên thứ ba
        logging.getLogger("pika").setLevel(logging.WARNING)
        logging.getLogger("mysql.connector").setLevel(logging.WARNING)

    except Exception as e:
        # Bắt các lỗi không xác định khác
        logging.error(f"Error: {e}")

# Hàm thiết lập RabbitMQ
def setup_rabbitmq_connection(config):
    try:
        rabbitmq_config = config["rabbitmq"]
        credentials = pika.PlainCredentials(
            rabbitmq_config["username"], rabbitmq_config["password"]
        )
        parameters = pika.ConnectionParameters(
            host=rabbitmq_config["host"],
            port=int(rabbitmq_config["port"]),
            virtual_host=rabbitmq_config.get("vhost", "/"),
            credentials=credentials,
            connection_attempts=3,
            retry_delay=5,
            socket_timeout=10,
            heartbeat=60,
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        # Khai báo queue 'Test' nếu chưa tồn tại
        channel.queue_declare(queue="Ticket_API")
        channel.queue_declare(queue="Ticket_Mail")
        logging.info("RabbitMQ connection and channel setup completed successfully.")
        return connection, channel
    except pika.exceptions.AMQPError as err:
        logging.error(f"Error setting up RabbitMQ connection: {err}")
        return None, None

import pandas as pd

def get_ticket(ticket_id):
    conn = connect_to_database(read=True)
    query = """
    SELECT * FROM jwdb.outbox_sp_tickets
    WHERE id = %s
    ORDER BY dateModified ASC LIMIT 1
    """

    try:
        df = pd.read_sql(query, conn, params=(ticket_id,))  # Truyền ticket_id vào query
    finally:
        conn.close()  # Đóng kết nối dù có lỗi hay không

    return df

def get_ticket_data_from(ticket_id):
    conn = connect_to_database(read=True)
    query = """
    SELECT * FROM jwdb.app_fd_sp_tickets_data
    WHERE c_fkTicket = %s
    ORDER BY dateModified ASC LIMIT 1
    """

    try:
        df = pd.read_sql(query, conn, params=(ticket_id,))  # Truyền ticket_id vào query
    finally:
        conn.close()  # Đóng kết nối dù có lỗi hay không

    if df.empty:
        return None
    return df

def get_ticket_data(ticket_id):
    conn = connect_to_database(read=True)
    query = """
    SELECT * FROM jwdb.app_fd_sp_tickets
    WHERE c_MaTicket = %s
    ORDER BY dateModified ASC LIMIT 1
    """

    try:
        df = pd.read_sql(query, conn, params=(ticket_id,))  # Truyền ticket_id vào query
    finally:
        conn.close()  # Đóng kết nối dù có lỗi hay không

    return df


def insert_ticket(df):
    try:
        conn = connect_to_database(read=False)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO outbox_sp_ticket_report ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))

        conn.commit()  # Quan trọng: lưu thay đổi vào database

    except Exception as e:
        logging.error(f"Error: {e}")
   
    finally:
        cursor.close()
        conn.close()  # Dùng close() thay vì dispose()
    

def insert_ticket_data(df):
    try:
        df["uuid"] = [str(uuid.uuid4()) for _ in range(len(df))]
        selected_columns = [
            "uuid",
            "c_trangThai",
            "modifiedByName",
            "dateModified",
            "c_huongxl",
            "c_content",
            "c_handle_time",
            "c_ngayDenHenYeuCau",
            "c_trangThaiPHKH",
            "c_fileUpload",
            "c_pheDuyet",
            "c_user_pheDuyet",
            "c_mail_to",
            "c_mail_from",
            "c_fileUpload_xl",
            "id",
            "c_messageId"
        ]

        df_select = df[selected_columns]
        conn = connect_to_database(read=False)
        cursor = conn.cursor()

        # for _, row in df.iterrows():
        #     placeholders = ', '.join(['%s'] * len(row))
        #     columns = ', '.join(row.index)
        #     sql = f"INSERT INTO jwdb.app_fd_sp_tickets_data ({columns}) VALUES ({placeholders})"
        #     cursor.execute(sql, tuple(row))

        placeholders = ', '.join(['%s'] * len(df_select.columns))
        columns = 'id, c_trangThai, modifiedByName, dateModified, c_huongxl, c_content, c_handle_time, ' \
                'c_ngayDenHenYeuCau, c_trangThaiPHKH, c_fileUpload, c_pheDuyet, c_user_pheDuyet, ' \
                'c_to, c_from, c_fileUpload_xl, c_fkTicket , c_messageId'

        sql = f"INSERT INTO jwdb.app_fd_sp_tickets_data ({columns}) VALUES ({placeholders})"
        data = [tuple(row) for row in df_select.to_numpy()]

        cursor.executemany(sql, data)
        conn.commit()  # Quan trọng: lưu thay đổi vào database

    except Exception as e:
        logging.error(f"Error: {e}")

    finally:
        cursor.close()
        conn.close()  # Dùng close() thay vì dispose()


# Get outbox_sp_tickets
def fetch_assignments():
    try:
        conn = connect_to_database(read=True)
        if conn is None:
            return pd.DataFrame()
        query = """
        SELECT
        id as ticket_index,
        ticket_id as id,
        coalesce(c_individual,'') as Nhom ,
        coalesce(c_PhanLoai,'') as L1,
        coalesce(c_NhomYeuCau,'') as L2,
        coalesce(c_DanhMucYeuCau,'') as L3,
        coalesce(c_ChiTietYeuCau,'') as L4,
        c_field15 as flag,
        coalesce(c_DonViGan,'') as don_vi_gan,
        createdBy,
        c_source,
        c_individual,
        c_CapDoXuLy,
        c_PhanLoai,
        c_NhomYeuCau,
        c_DanhMucYeuCau,
        c_ChiTietYeuCau,
        c_level5,
        c_DonViGan,
        c_DonViGan_CoDinh,
        c_MucDo,
        c_content,
        c_sla_phanHoiKH as sla_landau,
        c_sla_phanHoiKH,
        c_huongxl,
        c_user_pheDuyet,
        c_pheDuyet,
        dateModified as c_thoigian_Tiepnhan,
        dateModified,
        c_sla_dvxl as sla_chuyensau,
        c_sla_dvxl,
        c_cif,
        c_CIF,
        c_tenDn,
        c_tenDN,
        c_Phone,
        c_maTicket,
        c_fileUpload,
        modifiedBy,
        c_field20,
        dateCreated,
        c_type_ho,
        c_type_ticket_ho,
        c_api,
        c_status,
        c_transation_data,
        c_fkKH,
        c_trangThai
        -- ,c_companyCode,c_tenDn,c_noidung_denghi,c_Phone,c_thoigian_hoiketqua,dateCreated,c_ChiTietYeuCau,c_cif,c_content,c_DanhMucYeuCau,c_date_change_time,c_DonViGan,c_donViXuLy,c_fileUpload,c_handle_time,c_huongxl,c_individual,c_maTicket,c_nd_pheDuyet,c_ngayDenHenYeuCau,c_NhomYeuCau,c_pakh_time,c_PhanLoai,c_pheDuyet,c_phongBanXyLy,c_sla_dvxl,c_sla_phanHoiKH,c_source,c_time_xl,c_trangThai,c_trangThaiPHKH,c_user_pheDuyet
        FROM jwdb.outbox_sp_tickets
    WHERE c_status in ('Inserted','Updated') and coalesce(c_field2,'')<>'1' and coalesce(c_PhanLoai,'')<>'' and c_trangThai <> 'Đóng' and modifiedBy not in ('AI Nhỡ', 'AI BlockCard', 'AI Resetpass')
        ORDER BY dateModified ASC LIMIT 1
        """
        assignments = pd.read_sql(query, conn)
        conn.close()
        return assignments
    except mysql.connector.Error as err:
        logging.error(f"Error fetching assignments: {err}")
        return pd.DataFrame()

def get_don_vi_gan(cursor, don_vi_gan):
    try:
        don_vi_gan = don_vi_gan.split()[0]
        query = """
        SELECT loaihinh
        FROM jwdb.app_fd_etl_company
        WHERE %s like company_code
        """
        cursor.execute(query, (don_vi_gan,))
        result = cursor.fetchall()
        return result

    except Exception as e:
        logging.error(f"Error: {e}")
# SLA_ALL_LV3 =

# Match and return status in Excel
def match_and_fetch_status(rabbitmq_channel, assignment):
    try:
        conn = connect_to_database(read=False)
        if conn is None:
            logging.error("Không thể kết nối tới cơ sở dữ liệu để cập nhật.")
            return
        cursor = conn.cursor()
        query_sla = """
            SELECT
                COALESCE(c_tgian_xuly * 1, 0) + COALESCE(c_tgian_dong * 1, 0) + COALESCE(c_tgian_chuyentiep * 1, 0) AS sla,
                COALESCE(c_tgian_xuly, 0) AS sla_xuly,
                COALESCE(c_tong_ngay, 0) AS tong_ngay
            FROM
                jwdb.app_fd_su_sla_ticket
            WHERE
                COALESCE(c_PhanLoai, '') LIKE CONCAT('%', %s, '%')
                AND (
                    COALESCE(c_NhomYeuCau, '') LIKE CONCAT('%', %s, '%')
                    OR c_NhomYeuCau = ''
                )
                AND (
                    COALESCE(c_DanhMucYeuCau, '') LIKE CONCAT('%', %s, '%')
                    OR c_DanhMucYeuCau = ''
                )
                AND COALESCE(c_loai_sla, '') LIKE CONCAT('%', %s, '%')
                AND COALESCE(c_douutien_tk, '') LIKE CONCAT('%', %s, '%')
            ORDER BY
                CASE WHEN COALESCE(c_NhomYeuCau, '') LIKE CONCAT('%', %s, '%')
                AND COALESCE(c_DanhMucYeuCau, '') LIKE CONCAT('%', %s, '%') THEN 0 ELSE 1 END
            LIMIT 1;
            """
        logging.info(
            f"{assignment['L1']}, {assignment['L2']}, {assignment['L3']}, {assignment['c_CapDoXuLy']}, {assignment['c_MucDo']}")
        logging.info("-------------------------------------------")
        cursor.execute(
            query_sla,
            (assignment["L1"], assignment["L2"], assignment["L3"], assignment["c_CapDoXuLy"], assignment["c_MucDo"], assignment['L2'], assignment['L3']))
        result = cursor.fetchone()
        
        if result:
            columns = ["sla", "sla_xuly", "tong_ngay"]
            result_dict = dict(zip(columns, result))
            logging.info(f"Found SLA data: {result_dict}")
            sla = pd.to_numeric(result_dict["sla"], errors="coerce")
            sla_xuly = result_dict["sla_xuly"]
            tong_ngay = result_dict["tong_ngay"]
            logging.info(f"Found SLA: {sla}")
        else:
            logging.info("No SLA data found.")
            sla = 0
            sla_xuly = 0
            tong_ngay = 0
        # excel_file_path = "/home/Python/Template_xuly.xlsx"
        # excel_df = pd.read_excel(excel_file_path)
        # logging.info(excel_df.columns.tolist())
        try:
            don_vi_gan_list = get_don_vi_gan(cursor, assignment["don_vi_gan"])
            don_vi_gan = don_vi_gan_list[0][0] if don_vi_gan_list else ""
        except Exception as e:
            don_vi_gan = ""
            logging.error(f"Get don vi gan: {e}")
        # # Ensure all necessary columns are strings
        # for col in ["Nhom", "L1", "L2", "L3", "L4", "L5"]:
        #     excel_df[col] = excel_df[col].fillna("").astype(str).str.strip()
        #     logging.info(f" found '{col}' column into Excel.")
        # excel_Nhom = excel_df["Nhom"].str.split("+").str[0]
        # excel_L1 = excel_df["L1"].str.split("+").str[0]
        # excel_L2 = excel_df["L2"].str.split("+").str[0]
        # excel_L3 = excel_df["L3"].str.split("+").str[0]
        # excel_L4 = excel_df["L4"].str.split("+").str[0]

        # if assignment["L1"] == "Log-Lv1-000265":
        #     condition = (
        #             (excel_Nhom == assignment["Nhom"])
        #             & (excel_L1 == assignment["L1"])
        #             & (excel_L2 == assignment["L2"])
        #         )
        # elif assignment["L1"] == "Log-Lv1-000268" or assignment["L1"] == "Log-Lv1-000265":
        #     condition = (
        #             (excel_Nhom == assignment["Nhom"])
        #             & (excel_L1 == assignment["L1"])
        #             & (excel_L2 == assignment["L2"])
        #             & (excel_L3 == assignment["L3"])
        #     )
        # else:
        #     condition = (
        #             (excel_Nhom == assignment["Nhom"])
        #             & (excel_L1 == assignment["L1"])
        #             & (excel_L2 == assignment["L2"])
        #             & (excel_L3 == assignment["L3"])
        #             & (excel_L4 == assignment["L4"])
        #     )
        # logging.info(
        #     f"{assignment['Nhom']},{assignment['L1']},{assignment['L2']},{assignment['L3']},{assignment['L4']}")
        # # logging.info(f"{condition}")
        cursor.close()
        cursor = conn.cursor()
        try:
            procedure_call = "CALL msb_api.tk_get_action_by_level(%s, %s, %s, %s);"
            params = (assignment["L1"], assignment["L2"], assignment["L3"], '')
            logging.info(f"params{params}")
            cursor.execute(procedure_call, params)
            
            result_rows = cursor.fetchone()

            logging.info(f"abc {cursor.description}")
            columns = [col[0] for col in cursor.description]
            logging.info(f"result_rows {result_rows}")
            result_dicts = [dict(zip(columns,result_rows))]
        

            logging.info(f"result_dict {result_dicts}")
        except Exception as e:
            logging.error(f"get databasse error: {e}")
        
        _ = cursor.fetchall()
        # conn.commit()
        cursor.close()
        conn.close()
       
        if result_dicts:
            first_row = result_dicts[0]
            status = first_row.get("Status")
            email = first_row.get("Email")
            obj_type = first_row.get("Type")
            obj = first_row.get("Object")
            api = first_row.get("API")

            flag = assignment["flag"]
            logging.info(f"don vi gan: {don_vi_gan}")
            # jobcode = ""
            # if (
            #         "CN" in don_vi_gan.upper()
            #         and "JobcodeCN" in matched_rows.columns
            # ):
            #     jobcode = matched_rows["JobcodeCN"].iloc[0]
            # elif "PGD" in don_vi_gan.upper() and "JobcodePGD" in matched_rows.columns:
            #     jobcode = matched_rows["JobcodePGD"].iloc[0]

            # logging.info(f"L3 {matched_rows['L3']}")
            # while True:
            #     _ = cursor.fetchall()
            #     if cursor.nextset() is None:
            #         break
            list_email = []
            try:
                conn = connect_to_database(read=False)
                if conn is None:
                    logging.error("Không thể kết nối tới cơ sở dữ liệu để cập nhật.")
                    return
                cursor = conn.cursor()
                while cursor.nextset():
                    cursor.fetchall()  
                logging.info(f"{first_row.get('Nhom')} {first_row.get('L1')} {first_row.get('L2')} {first_row.get('L3')}")
                list_email = get_list_email(
                    assignment['Nhom'],
                    first_row.get('L1'),
                    first_row.get('L2'),
                    first_row.get('L3'),
                    cursor,
                    assignment,
                    don_vi_gan,
                    assignment["don_vi_gan"],
                    logging
                )
                # test
                logging.info(f"list_email:{list_email}")
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                logging.error(f"get list_mail error: {e}")

            update_ticket_status_main(
                ticket_id=assignment["id"],
                status=status,
                status_ticket = assignment["c_trangThai"],
                email=email,
                obj_type=obj_type,
                obj=obj,
                flag_email=flag,
                api=api,
                list_email = list_email,
                jobcode="",
                createdBy=assignment["createdBy"],
                rabbitmq_channel=rabbitmq_channel,
                assignment=assignment,
                sla=sla,
                sla_xuly=sla_xuly,
                tong_ngay=tong_ngay,
            )
            return {"status": status, "email": email, "type": obj_type, "object": obj}, sla_xuly, True if assignment[
                                                                                                              "c_CapDoXuLy"] == "Xử lý lần đầu" else False
        else:
            # logging.info(excel_Nhom, excel_L1, excel_L2, excel_L3, excel_L4)
            logging.info("No match database.")
            ticket_id = assignment["id"].iloc[0] if isinstance(assignment["id"], pd.Series) else assignment["id"]
            # update_sla_manual(cursor, ticket_id, sla, sla_xuly, tong_ngay, assignment, logging)
            logging.info(f"Ticket {ticket_id} đã được cập nhật sla đóng.")

       
        return None, sla_xuly, True if assignment["c_CapDoXuLy"] == "Xử lý lần đầu" else False
    except Exception as e:
        logging.error(f"Error in match_and_fetch_status: {e}")
        return None, None, None


def update_ticket_status_main(
        ticket_id,
        status,
        status_ticket,
        email,
        obj_type,
        obj,
        flag_email,
        api,
        list_email,
        jobcode,
        createdBy,
        rabbitmq_channel,
        assignment,
        sla,
        sla_xuly,
        tong_ngay,
        requests=None,
):
    try:
        logging.info(f"Start updating.....")
        if status and status != "":
            conn = connect_to_database(read=False)
            if conn is None:
                logging.error("Không thể kết nối tới cơ sở dữ liệu để cập nhật.")
                return

            cursor = conn.cursor()
            logging.info(f"[*] Handle: {obj_type} - { assignment.get('c_type_ticket_ho', '')} - "
                         f"{assignment.get('c_status', '')} ")
            
            sla_phanHoi = ""
            try:
                if sla != 0:
                    if assignment['L1'] == "Log-Lv1-000265":
                        sla_phanHoi = update_sla_manual(cursor, ticket_id, sla, sla_xuly, tong_ngay, assignment, logging, mode="weekday", include_saturday=True)
                    elif assignment['L1'] == "Log-Lv1-000273":
                        sla_phanHoi = update_sla_manual(cursor, ticket_id, sla, sla_xuly, tong_ngay, assignment, logging, mode="full_day", include_saturday=False)
                    else:
                        sla_phanHoi = update_sla_manual(cursor, ticket_id, sla, sla_xuly, tong_ngay, assignment, logging)
                conn.commit()
                cursor.close()
                conn.close()
            except  Exception as e:
                logging.error(f"Error sla: {e}", exc_info=True)

            conn = connect_to_database(read=False)
            if conn is None:
                logging.error("Không thể kết nối tới cơ sở dữ liệu để cập nhật.")
                return

            cursor = conn.cursor()

            # logging.info(cursor)
            if assignment["c_CapDoXuLy"] in ["Xử lý lần 2", "Xử lý lần 3", "Xử lý lần 4"]:
                if assignment['L1'] == "Log-Lv1-000265":
                    update_followup_processing(cursor, ticket_id, assignment, logging, mode="weekday", include_saturday=True)
                elif assignment['L1'] == "Log-Lv1-000273":
                    update_followup_processing(cursor, ticket_id, assignment, logging, mode="full_day", include_saturday=False)
                else:
                    update_followup_processing(cursor, ticket_id, assignment, logging)
            elif assignment.get('c_type_ticket_ho', '') in ["Tra soát", "Tra soát All", "Rủi ro", "Rủi ro All"]:
                handle_ho_auto(assignment, api, status, ticket_id, rabbitmq_channel, logging, conn)

            elif obj_type == "Thủ công":
                handle_manual_ticket(conn, cursor, ticket_id, status, sla, sla_xuly, tong_ngay, assignment, flag_email, api,
                                     rabbitmq_channel, obj, logging)
            elif obj_type == "Tự động":
                handle_automatic_ticket(conn, jobcode, createdBy, email, cursor, ticket_id, status, status_ticket, sla, sla_xuly, tong_ngay, assignment, api, rabbitmq_channel, obj, list_email, sla_phanHoi, logging)




            conn.commit()
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        logging.error(f"Error updating ticket {ticket_id}: {err}")
    except pika.exceptions.AMQPError as rabbit_err:
        logging.error(f"Error sending message to RabbitMQ: {rabbit_err}")


def recall_data(rabbitmq_channel):
    try:

        logging.info("insert_tciket done")
        assignments = fetch_assignments()
        if assignments.empty:
            logging.info("No assignments to recall.")
            return

        for _, assignment in assignments.iterrows():
            try:
                maTicket = assignment["c_maTicket"] if not isinstance(assignment["c_maTicket"], pd.Series) else assignment["c_maTicket"].iloc[0]
                df_ticket = get_ticket_data(maTicket)
                Ticket_id =df_ticket["id"] if not isinstance(df_ticket["id"], pd.Series) else df_ticket["id"].iloc[0]

                ticket_data = get_ticket_data_from(Ticket_id)
                if ticket_data is None:
                    logging.info("start insert ticket mới")
                    insert_ticket_data(df_ticket)
                else:
                    logging.info("không phải là ticket mới")
            except Exception as e:
                logging.error(f"Error in recall_data: {e}", exc_info=True)
            assignment_id = assignment["id"]
            logging.info(f"Processing assignment ID: {assignment_id}")

            status_data, sla, check_xu_ly_lan_dau = match_and_fetch_status(rabbitmq_channel, assignment)
            if status_data:
                logging.info(f"Matched and updated ticket with status: {status_data}")
            else:
                logging.info(f"No match found for assignment ID: {assignment_id}")
            update_assignment_status(assignments, sla, check_xu_ly_lan_dau)
            logging.info(
                f"Recalled data and updated status for assignment ID: {assignment_id}"
            )


    except Exception as e:
        logging.error(f"Error in recall_data: {e}", exc_info=True)


##  data_label_for_level = ["c_PhanLoai","c_NhomYeuCau","c_DanhMucYeuCau","c_ChiTietYeuCau"]
def retrieve_id_for_level(level: str, data: str) -> str:
    try:
        conn = connect_to_database(read=False)
        if conn is None:
            return
        cursor = conn.cursor()
        if level == "c_PhanLoai":
            query = """
            SELECT c_ten_phan_loai
            FROM jwdb.app_fd_sp_phanloai
            WHERE id = %s
            """
        elif level == "c_NhomYeuCau":
            query = """
            SELECT c_ten_nhom_yeucau
            FROM jwdb.app_fd_sp_nhomyeucau
            WHERE id = %s
            """
        elif level == "c_DanhMucYeuCau":
            query = """
            SELECT c_ten_danh_muc_yeucau
            FROM jwdb.app_fd_sp_danhmucyeucau
            WHERE id = %s
            """
        else:
            query = """
            SELECT c_ten_level_4
            FROM jwdb.app_fd_sp_ql_lv4
            WHERE id = %s
            """
        cursor.execute(query, (data,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return None
    except mysql.connector.Error as err:
        logging.error(f'Error updating assignment status for id {level}: {err}')


def update_assignment_status(assignment, sla, check_xuly_lan_dau):
    try:
        conn = connect_to_database(read=False)
        if conn is None:
            return
        cursor = conn.cursor()
        query_before = """
            SELECT c_source,
                c_individual,
                c_CapDoXuLy,
                c_PhanLoai,
                c_NhomYeuCau,
                c_DanhMucYeuCau,
                c_ChiTietYeuCau,
                c_level5,
                c_DonViGan,
                c_MucDo,
                c_content,
                modifiedBy,
                dateModified,
                c_sla_phanHoiKH,
                c_huongxl,
                c_user_pheDuyet,
                c_pheDuyet
            FROM jwdb.outbox_sp_tickets
            WHERE ticket_id = %s
            AND dateModified < %s
            ORDER BY dateModified DESC
            LIMIT 1
        """
        ticket_id = assignment["id"] if not isinstance(assignment["id"], pd.Series) else assignment["id"].iloc[0]
        date_before = assignment["dateModified"] if not isinstance(assignment["dateModified"], pd.Series) else \
        assignment["dateModified"].iloc[0]

        assignments_before = pd.read_sql(query_before, conn, params=(ticket_id, date_before,))
        logging.info(f"affect rows: {assignments_before}")
        note = ""

        if assignments_before.empty:
            logging.info(f"No previous record found for ticket_id {assignment['id']} - {ticket_id}")
        else:
            column_labels = {
                "c_source": "Nguồn",
                "c_individual": "Nhóm KH",
                "c_CapDoXuLy": "Cấp độ xử lý",
                "c_PhanLoai": "Level 1 - Phân loại",
                "c_NhomYeuCau": "Level 2 - Nhóm yêu cầu",
                "c_DanhMucYeuCau": "Level 3",
                "c_ChiTietYeuCau": "Level 4",
                "c_level5": "Level 5",
                "c_DonViGan": "Đơn vị gán",
                "c_MucDo": "Mức độ ưu tiên",
                "c_content": "Nội dung xử lý",
                "modifiedBy": "User thay đổi",
                "dateModified": "Thời gian thay đổi",
                "c_sla_phanHoiKH": "SLA phản hồi",
                "c_huongxl": "Nội dung xử lý",
                "c_user_pheDuyet": "User phê duyệt",
                "c_pheDuyet": "Trạng thái phê duyệt"
            }

            differences = []
            data_label_for_level = ["c_PhanLoai", "c_NhomYeuCau", "c_DanhMucYeuCau", "c_ChiTietYeuCau"]

            for col in column_labels.keys():
                old_value = assignments_before.iloc[0][col]
                if col != "c_sla_phanHoiKH":
                    new_value = assignment[col] if isinstance(assignment[col], (str, int, float)) else \
                    assignment[col].iloc[
                        0]
                else:
                    new_value = sla if check_xuly_lan_dau else old_value
                if col in data_label_for_level:
                    old_value = retrieve_id_for_level(col, old_value)
                    new_value = retrieve_id_for_level(col, new_value)
                # logging.info(f"Old value: {old_value}, New value: {new_value}")
                if old_value != new_value:
                    differences.append(
                        {
                            "label": column_labels[col],
                            "old_value": old_value,
                            "new_value": new_value
                        })
            agent = assignment.iloc[0]["modifiedBy"]
            if not differences:
                note = "<p>Không có trường nào thay đổi.</p>"
            else:
                note = "<p>Các trường đã thay đổi:</p>"
                note += "<table border='1' cellpadding='5' cellspacing='0'>"
                note += "<tr><th>Label</th><th>Giá trị cũ</th><th>Giá trị mới</th><th>Agent thay đổi</th></tr>"  # Tiêu đề bảng

                for diff in differences:
                    note += f"<tr><td>{diff['label']}</td><td>{diff['old_value']}</td><td>{diff['new_value']}</td><td>{agent}</td></tr>"

                note += "</table>"

        logging.info(note)
        query = """
        UPDATE jwdb.outbox_sp_tickets
        SET c_status = CONCAT(c_status, '-Done'),c_field20 = %s
        WHERE id = %s
        """
        # query_update = """
        # UPDATE jwdb.outbox_sp_ticket_report
        # SET c_status = CONCAT(c_status, '-Done'),c_field20 = %s
        # WHERE id = %s
        # """
        ticket_index = assignment["ticket_index"] if not isinstance(assignment["ticket_index"], pd.Series) else \
            assignment["ticket_index"].iloc[0]
        ticket_index = int(ticket_index)
        logging.info(f'Executing query: {query} with id: {assignment["ticket_index"]}')
        cursor.execute(query, (note, ticket_index,))
        # cursor.execute(query_update, (note, ticket_index,))


        # query = """
        #     UPDATE jwdb.outbox_sp_tickets
        #     SET c_status = CONCAT(c_status, '-Done')
        #     WHERE ticket_id = %s
        # """
        # ticket_id = assignment["id"] if not isinstance(assignment["id"], pd.Series) else \
        #     assignment["id"].iloc[0]
        # logging.info(f'Executing query: {query} with id: {assignment["id"]}')
        # cursor.execute(query, (ticket_id,))

        logging.info(f"Rows affected: {cursor.rowcount}")
        conn.commit()
        cursor.close()
        conn.close()

        df = get_ticket(ticket_index)
        insert_ticket(df)
        logging.info("update ticket data report")
        maTicket = assignment["c_maTicket"] if not isinstance(assignment["c_maTicket"], pd.Series) else \
            assignment["c_maTicket"].iloc[0]
        df_ticket = get_ticket_data(maTicket)
        # if df["c_trangThai"].iloc[0] != df_ticket["c_trangThai"].iloc[0] or df_ticket["c_trangThai"].iloc[0] == "Mới":
        logging.info("update ticket data")
        insert_ticket_data(df_ticket)
        logging.info("update ticket data done")
    except mysql.connector.Error as err:
        logging.error(f'Error updating assignment status for id {assignment["id"]}: {err}', exc_info=True)


if __name__ == "__main__":
    setup_logging()
    while True:
        current_time = time.time()
        lock_acquired = False
        try:
            if os.path.exists(LOCK_FILE):
                lockfile_age = current_time - os.path.getmtime(LOCK_FILE)
                if lockfile_age > LOCK_TIMEOUT:
                    os.remove(LOCK_FILE)
                    logging.info("Old lockfile removed due to timeout.")

            with open(LOCK_FILE, "w") as lockfile:
                try:
                    fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lock_acquired = True

                    logging.info("Start Manual Data Recall...")
                    os.utime(LOCK_FILE, None)

                    rabbitmq_config = read_config(
                        os.path.join(
                            os.path.dirname(__file__), "common_config", "config.ini"
                        )
                    )
                    _, rabbitmq_channel = setup_rabbitmq_connection(rabbitmq_config)

                    recall_data(rabbitmq_channel)

                except IOError:
                    logging.error("Lock already acquired by another process. Exiting.")
                    continue

        except Exception as e:
            logging.error(f"Error: {e}")

        finally:
            if lock_acquired:
                with open(LOCK_FILE, "w") as lockfile:
                    fcntl.flock(lockfile, fcntl.LOCK_UN)
                logging.info("Lock released. Task completed.")
                os.remove(LOCK_FILE)

        time.sleep(SLEEP_INTERVAL)
