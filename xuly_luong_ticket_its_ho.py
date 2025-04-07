import json
from datetime import datetime
import logging

def update_ticket_email(cursor, ticket_id, ticket_email):
    query = """
        UPDATE jwdb.app_fd_sp_tickets
        SET c_ticket_mail = %s
        WHERE id = %s
        """
    try:
        cursor.execute(query, (ticket_email, ticket_id))
   
    except Exception as e:
        # Bắt tất cả các lỗi không xác định
        logging.error(f"Error: {e}")

def get_email_template(cursor, template, logging):
    """
    Lấy template email từ database.
    """
    try:
        ma_template = template.split(" - ", 1)[-1]
        logging.info(f"ma_template {ma_template}")
        query = """
            SELECT c_tieu_de AS tieu_de, c_content AS content
            FROM jwdb.app_fd_sp_template_email
            WHERE c_ma_tempalet = %s
        """
        cursor.execute(query, (ma_template,))
        template_result = cursor.fetchone()

        if template_result:
            # tieu_de = template_result[0]
            # content = template_result[1]
            # logging.info(f"{tieu_de} {content}")
            return template_result[0], template_result[1]

    except Exception as e:
        # Bắt tất cả các lỗi không xác định
        logging.error(f"Error: {e}")
    
def fill_template(content, data, logging):
    """
    Thay thế placeholder trong nội dung email bằng dữ liệu thực tế từ `assignment`.
    """
    try:
        return content.format(**data)
    
    except KeyError as e:
        logging.error(f"Lỗi: Thiếu trường {e} trong dữ liệu để thay vào template.")
        return content


def handle_template(cursor, template, data_map, data_tieude, logging):
    try:
        tieu_de, content = get_email_template(cursor, template, logging)
        logging.info(f"content {content}")
        logging.info(f"template {template}")
        content_data = fill_template(content, data_map, logging)
        tieu_de_data = fill_template(tieu_de, data_tieude, logging)
        logging.info(f"content_data: {content_data}")
        return content_data, tieu_de_data
    
    except Exception as e:
        # Bắt tất cả các lỗi không xác định
        logging.error(f"Error: {e}")
   

def handler_its_ho(assignment, api, status, ticket_id, rabbitmq_channel, logging, conn):

    try:
        ticket_email = ''
        if api == "ITS":
            ticket_email = "ITS"
        elif api == "HO":
            if assignment["c_PhanLoai"] == "Log-Lv1-000272":
                ticket_email = "HO-Tra soat"
            elif assignment["c_PhanLoai"] == "Log-Lv1-000274":
                ticket_email = "HO-Rui ro"
            else:
                ticket_email = "HO"
        update_ticket_email(conn.cursor(), ticket_id, ticket_email)
        conn.commit()

        if len(status.split("+")) < 3:
            logging.warn(f"[!] Ticket {assignment.get('c_maTicket')} has invalid status to send to API - {status}")
            return
        cursor = conn.cursor()

        ma_ticket = assignment["c_maTicket"] if assignment["c_maTicket"] else ""

        logging.info(f"Start sending message to queue Ticket_API")
        l1_id = assignment["c_PhanLoai"]
        l2_id = assignment["c_NhomYeuCau"]
        l3_id = assignment["c_DanhMucYeuCau"]
        l4_id = assignment["L4"]
        l1, l2, l3, l4 = fetch_data(cursor, l1_id, l2_id, l3_id, l4_id)
        muc_do = assignment["c_MucDo"]
        cap_do_xu_ly = assignment["c_CapDoXuLy"]
        sla = assignment["sla_landau"] if cap_do_xu_ly == "Xử lý lần đầu" else assignment["sla_chuyensau"]
        cif = assignment["c_cif"]
        customer_name = assignment["c_tenDn"]
        phone_number = assignment["c_Phone"]
        content = assignment["c_content"]
        attachment = assignment["c_fileUpload"]
        don_vi_gan = assignment["c_DonViGan"]

        thoi_gian_tiep_nhan = assignment["c_thoigian_Tiepnhan"]
        date_modified_str = assignment.get("c_thoigian_Tiepnhan", "")
        date_modified = datetime.strftime(
            datetime.strptime(date_modified_str, '%Y-%m-%d %H:%M:%S'),
            '%d/%m/%Y %H:%M') if date_modified_str else ""

        ten_dn = assignment["c_tenDn"] if assignment["c_tenDn"] else ""
        # data_chung map template
        data_map = {
            "MucDo": muc_do,
            "CapDoXuLy": cap_do_xu_ly,
            "thoigian_Tiepnhan": date_modified,
            "sla": sla,
            "cif": cif,
            "DonViGan": don_vi_gan,
            "sla_phanHoiKH": "",
            "thoigian_Tiepnhan": thoi_gian_tiep_nhan,
            
            "customerName": customer_name,
            "phoneNumber": phone_number,
            "NhomYeuCau": l2,
            "DanhMucYeuCau": l3,
            "content": content,
            "phone_number": phone_number,
        }
        data_tieude_map = {
            "maTicket": ma_ticket,
            "customerName": customer_name,
            "companyCode" : don_vi_gan,
        }

        subject = f"{ma_ticket}"
        description = f"{content}"
        logging.info(f"api {api}")
        if api == "ITS":
            data = {
                "MucDo": muc_do,
                "CapDoXuLy": cap_do_xu_ly,
                "thoigian_Tiepnhan": date_modified,
                "sla": sla,
                "cif": cif,
                "customerName": customer_name,
                "phoneNumber": phone_number,
                "NhomYeuCau": l2,
                "DanhMucYeuCau": l3,
                "content": content,
                "phone_number": phone_number,
            }
            file_template_path = "/home/Python/template/ITs.txt"
            description = get_template(file_template_path, data)
            subject = f"[Tổng đài CSKH - {ma_ticket}] - {l1} - {l2} - {ten_dn}"

        else:
            if api == "HO SUPPORT":
                # Tra soát thẻ | Tra soát CK | Nhận tiền về
                if l1 == 'Tra soát':
                    subject = f"[Tổng đài CSKH - {ma_ticket}] - Tra soát - {l3} - {ten_dn}"

                    if l2 == "Thẻ ghi nợ nội địa" or l2 == "Thẻ ghi nợ quốc tế" or l2 == "Thẻ tín dụng quốc tế":
                        description, subject = handle_template(cursor, 'HO2', data_map, data_tieude_map, logging)
                        ## get template HO2

                    elif l3 == "Nhận tiền về":
                        ## get template HO4
                        description, subject = handle_template(cursor, 'HO4', data_map, data_tieude_map, logging)

                    ## get template HO1_Tra soat CK
                    else:
                        description, subject = handle_template(cursor, 'HO1', data_map, data_tieude_map, logging)


                # Rủi ro giao dịch:
                elif l1 == 'Rủi ro':
                    subject = f"[Tổng đài CSKH - {ma_ticket}] - Rủi ro GD - {l3} - {ten_dn}"

                    if l2 == "App LPBank" or l2 == "App LPBank BIZ":
                        ## get template HO5
                        description, subject = handle_template(cursor, 'HO5', data_map, data_tieude_map, logging)

                    elif l2 == "Dịch vụ Thẻ":
                        ## get template H06
                        description, subject = handle_template(cursor, 'HO6', data_map, data_tieude_map, logging)

                # Góp ý
                elif l1_id == 'Log-Lv1-000267' and (l2_id == 'Log-Lv2-000279'):
                    subject = f"[Tổng đài CSKH - {ma_ticket}] - Góp ý - {l3}"
                    data = {
                        "DonViGan": don_vi_gan,
                        "thoigian_Tiepnhan": date_modified,
                        "cif": cif,
                        "customerName": customer_name,
                        "phoneNumber": phone_number,
                        "NhomYeuCau": l3,
                        "DanhMucYeuCau": l4,
                        "content": content,
                    }
                    description = get_template("/home/Python/template/15_Gopy_ho.txt", data)

                # Khác
                else:
                    subject = f"[Tổng đài CSKH - {ma_ticket}] - {l1} - {l2} - {ten_dn}"

        logging.info(f"description: {description}")
        
        message = json.dumps(
            {
                "type": api,
                "ticket": ticket_id,
                "template": "",
                "message": status,
                "attachments": attachment,
                "subject": subject,
                "description": description,
                "phone_number": phone_number,
            }
        )

        utf8_message = message.encode("utf-8")
        rabbitmq_channel.basic_publish(
            exchange="", routing_key="Ticket_API", body=utf8_message
        )
        logging.info(
            f"[>] Send to queue [Ticket_API]: {utf8_message[:255]}{'...' if len(utf8_message) > 255 else ''}")

        cursor.close()

        cursor_update = conn.cursor()
        # update_flag_api(cursor_update, ticket_id, api)
        conn.commit()

    except Exception as e:
        logging.error(f"[!] Fail to send to Ticket_API queue: {e}", exc_info=True)


def fetch_data(cursor, phanloai_id, nhomyeucau_id, danhmucyeucau_id, level4):
    try:
        query_l1 = """
            SELECT a.c_ten_phan_loai
            FROM jwdb.app_fd_sp_phanloai a
            WHERE id = %s
            """
        cursor.execute(query_l1, (phanloai_id,))
        result_1 = cursor.fetchone()
        l1 = result_1[0] if result_1 else ""

        query_l2 = """
            SELECT n.c_ten_nhom_yeucau
            FROM jwdb.app_fd_sp_nhomyeucau n
            WHERE id = %s
            """
        cursor.execute(query_l2, (nhomyeucau_id,))
        result_2 = cursor.fetchone()
        l2 = result_2[0] if result_2 else ""

        query_l3 = """
            SELECT d.c_ten_danh_muc_yeucau
            FROM jwdb.app_fd_sp_danhmucyeucau d
            WHERE id = %s
            """
        cursor.execute(query_l3, (danhmucyeucau_id,))
        result_3 = cursor.fetchone()
        l3 = result_3[0] if result_3 else ""

        if level4:
            query_l4 = """
                SELECT d.c_ten_level_4
                FROM jwdb.app_fd_sp_ql_lv4 d
                WHERE id = %s
                """
            cursor.execute(query_l4, (level4,))
            result_4 = cursor.fetchone()
            l4 = result_4[0] if result_4 else ""
        else:
            l4 = ""

        return l1, l2, l3, l4

    except Exception as e:
        logging.error(f"Error: {e}")


def get_template(template_path, data):
    try:
        with open(template_path, "r", encoding="utf-8") as file:
            html_template = file.read()

        description = html_template.format(**data)
        return description

    except Exception as e:
        logging.error(f"Error: {e}")