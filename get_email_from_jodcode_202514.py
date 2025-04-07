import re
import logging
def get_ticket_by_maticket(cursor, maticket):
    query = """
    SELECT c_DonViGan, c_phanLoai, c_DanhMucYeuCau, c_NhomYeuCau, c_individual
    FROM jwdb.app_fd_sp_tickets WHERE c_MaTicket = %s
    """
    
    cursor.execute(query, (maticket,))
    result = cursor.fetchone()
    if result is None:
        return None  # Không có dữ liệu

    # Lấy danh sách tên cột từ cursor.description
    columns = [col[0] for col in cursor.description]

    # Chuyển tuple thành dictionary
    return dict(zip(columns, result))

def fetch_data(cursor, phanloai_id, nhomyeucau_id, danhmucyeucau_id):
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

    return l1, l2, l3
    
def get_recipient_emails_no_don_vi_gan(cursor, jobcode, logging):
    """
    Lấy danh sách email của người nhận dựa trên jobcode.
    """
    # new_don_vi_gan = don_vi_gan.split("-")[0].strip()
    # logging.info(f"don_vi_gan: {new_don_vi_gan}")
    cursor.execute("SET SESSION group_concat_max_len = 9999999999;")
    logging.info(f"jobcode:  {jobcode}")
    jobcode_list = jobcode.split(";")  # Tách thành danh sách
    placeholders = ",".join(["%s"] * len(jobcode_list))  # Tạo placeholders (%s, %s, ...)
    logging.info(f"placeholders {placeholders}")
    query = f"""
        SELECT GROUP_CONCAT(DISTINCT m.email SEPARATOR ';') AS email
        FROM jwdb.app_fd_etl_email m
        WHERE m.jobcode IN ({placeholders}) ;
    """
    logging.info(f"placeholders {placeholders}")
    # params = tuple(jobcode_list) + (new_don_vi_gan,)  

    cursor.execute(query, jobcode_list) 
    result = cursor.fetchone()    
    return result[0] if result[0] else None

    
def get_email_from_SPDV(L1, L2, L3, Nhom, logging):
    email_map = {
        ("Sản phẩm dịch vụ", "Khiếu nại", "App LPBank"): "p.kdthe.nhs@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "App LPbank Biz"): "nhdn.pgp@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Dịch vụ Thẻ"): "p.kdthe.nhs@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Bảo hiểm"): "p.kdbh.nhbl@lpbank.com.vn",

        # Các mục có nhóm riêng biệt
        ("Sản phẩm dịch vụ", "Khiếu nại", "Tín dụng", "Doanh nghiệp"): "nhdn.pgp@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Tín dụng", "Cá nhân"): "P.PTSP&GPTaichinh.nhbl@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Huy động", "Doanh nghiệp"): "nhdn.pgp@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Huy động", "Cá nhân"): "P.PTSP&GPTaichinh.nhbl@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Sản phẩm khác", "Doanh nghiệp"): "nhdn.pgp@lpbank.com.vn",
        ("Sản phẩm dịch vụ", "Khiếu nại", "Sản phẩm khác", "Cá nhân"): "P.PTSP&GPTaichinh.nhbl@lpbank.com.vn",
    }

    # Kiểm tra nếu có nhóm
    email = email_map.get((L1, L2, L3, Nhom))
    
    # Nếu không có nhóm thì lấy email mặc định theo L1, L2, L3
    if not email:
        email = email_map.get((L1, L2, L3))
    logging.info(f"{L1} {L2} {L3} {Nhom}")
    return [email]

def get_email_RR(cursor, logging):
    ma_jodcode = "HS2125;HS2126;HS2127;HS2142;HS2143;HS2157"
    email = get_recipient_emails_no_don_vi_gan(cursor, ma_jodcode, logging)
    email_list = email.split(";") if email else []
    email_list.extend(["p.ktgsnb@lpbank.com.vn", "b.rrhd@lpbank.com.vn", "p.dvkh247@lpbank.com.vn"])
    return email_list

def get_jobcode(donvigan, jodcode_key, cursor, logging):
    new_donvigan = 'Chi nhánh' if donvigan == 'CN'  or donvigan == 'PGDL' else 'Phòng giao dịch' if donvigan == 'PGD' else None
    query = """
    SELECT c_tong_hop_jobcode,c_tong_hop_jobcode_cha
    FROM jwdb.app_fd_sp_jobcode
    WHERE c_loai_donvi = %s
    AND c_don_vi = %s
    LIMIT 1;
    """
    
    # Execute the query with the provided parameters
    cursor.execute(query, (new_donvigan, jodcode_key))
    
    # Fetch the result
    result = cursor.fetchone()
    
    # Check if a result is found and return the value of c_tong_hop_jobcode
    if result:
        logging.info(f"jobcode: {result[0]}")
        return result[0], result[1]  # Assuming the jobcode is the first column in the result
    else:
        return None  # Or handle it if no result is found

def get_email_NPV(L1, L2, logging):
    email_map = {
        ("Ngoài phạm vi", "Truyền thông"): "truyenthong@lpbank.com.vn",
        ("Ngoài phạm vi", "Tuyển dụng"): "tuyendung@lpbank.com.vn",
        ("Ngoài phạm vi", "XDCB"): "ptml_xdcb@lpbank.com.vn",
    }

    # Kiểm tra nếu có nhóm
    email = email_map.get((L1, L2))
    
    # Nếu không có nhóm thì lấy email mặc định theo L1, L2, L3
    logging.info(f"{L1} {L2}")
    return [email]

def get_recipient_emails(cursor, jobcode, don_vi_gan, logging):
    """
    Lấy danh sách email của người nhận dựa trên jobcode.
    """
    
    cursor.execute("SET SESSION group_concat_max_len = 9999999999;")
    logging.info(f"jobcode:  {jobcode}")
    jobcode_list = re.split(r"[;,]", jobcode) # Tách thành danh sách
    placeholders = ",".join(["%s"] * len(jobcode_list))  # Tạo placeholders (%s, %s, ...)
    logging.info(f"placeholders {placeholders}")
    query = f"""
        SELECT GROUP_CONCAT(DISTINCT m.email SEPARATOR ';') AS email
        FROM jwdb.app_fd_etl_company c
        JOIN jwdb.app_fd_etl_email m ON c.company_code = m.madonvi
        -- JOIN jwdb.app_fd_etl_cf_jobcode j ON m.jobcode = j.JOBCODE
        WHERE m.jobcode IN ({placeholders}) AND c.company_code = %s
    """
    logging.info(f"placeholders {placeholders}")
    params = tuple(jobcode_list) + (don_vi_gan,)  

    cursor.execute(query, params) 
    result = cursor.fetchone()
    logging.info(f"result {result}")
    return result[0] if result[0] else ""

def get_don_vi_gan_cha(cursor, donvigan ):
    logging.info(f"{donvigan}")
    query = """
    select com_manager 
    from jwdb.app_fd_etl_company where company_code = %s
    """
    
    cursor.execute(query, (donvigan,))
    
    # Fetch the result
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        return None  # 

def get_email_TV(Nhom, donvigan, cursor, logging):
    try:
        don_vi_gan_list = get_don_vi_gan(cursor, donvigan)
        don_vi_gan = don_vi_gan_list[0][0] if don_vi_gan_list else ""
        new_don_vi_gan = donvigan.split("-")[0].strip()
        logging.info(f"don_vi_gan: {new_don_vi_gan}")
    except Exception as e:
        don_vi_gan = ""
        new_don_vi_gan = ""
        logging.error(f"Get don vi gan: {e}")
    if Nhom == "Cá nhân":
        jobcode, jobcode_cha = get_jobcode(don_vi_gan, "KHCN", cursor, logging)
    elif Nhom == "Doanh nghiệp":
        jobcode, jobcode_cha = get_jobcode(don_vi_gan, "KHDN", cursor, logging)
        
    if jobcode is None:
        logging.error(f"Nhom '{Nhom}' không hợp lệ hoặc không có jobcode.")
        return []
    
    logging.info(f"{don_vi_gan} {new_don_vi_gan}")

    email = get_recipient_emails(cursor, jobcode, new_don_vi_gan, logging)
    
    if jobcode_cha:
        logging.info("start")
        donvigan_cha = get_don_vi_gan_cha(cursor, new_don_vi_gan)
        logging.info(f"donvigan_cha {donvigan_cha}")
        email_cha = get_recipient_emails(cursor, jobcode_cha, donvigan_cha, logging)
        email_list = email.split(";") if email else []
        email_cha_list = email_cha.split(";") if email_cha else []
    else:
        email_list = email.split(";") if email else []
        email_cha_list = []

    merged_email_list = list(set(email_list + email_cha_list))
    
    return merged_email_list

def get_from_L3(L3, donvigan, cursor, logging):
    try:
        don_vi_gan_list = get_don_vi_gan(cursor, donvigan)
        don_vi_gan = don_vi_gan_list[0][0] if don_vi_gan_list else ""
        new_don_vi_gan = donvigan.split("-")[0].strip()
        logging.info(f"don_vi_gan: {new_don_vi_gan}")
    except Exception as e:
        new_don_vi_gan = ""
        don_vi_gan = ""
        logging.error(f"Get don vi gan: {e}")
    logging.info(f"l3 {L3}")
    logging.info(f"don_vi_gan {don_vi_gan}")
    if L3 in ["Vận hành", "Dịch vụ khách hàng"]:
        jobcode,jobcode_cha = get_jobcode(don_vi_gan, "DVKH", cursor, logging)
    elif L3 == "KHCN":
        jobcode,jobcode_cha = get_jobcode(don_vi_gan, "KHCN", cursor, logging)
    elif L3 == "KHDN":
        jobcode,jobcode_cha = get_jobcode(don_vi_gan, "KHDN", cursor, logging)
    elif L3 == "Tổng đài viên":
        return ["p.dvkh247@lpbank.com.vn"]
    if jobcode is None:
        logging.error(f"L3 '{L3}' không hợp lệ hoặc không có jobcode.")
        return []

    logging.info(f"{don_vi_gan} {new_don_vi_gan}")
    email = get_recipient_emails(cursor, jobcode, new_don_vi_gan, logging)
    if jobcode_cha:
        logging.info("start")
        donvigan_cha = get_don_vi_gan_cha(cursor, new_don_vi_gan)
        logging.info(f"donvigan_cha {donvigan_cha}")
        email_cha = get_recipient_emails(cursor, jobcode_cha, donvigan_cha, logging)
        email_list = email.split(";") if email else []
        email_cha_list = email_cha.split(";") if email_cha else []
    else:
        email_list = email.split(";") if email else []
        email_cha_list = []

    merged_email_list = list(set(email_list + email_cha_list))
    
    return merged_email_list    
    

def get_don_vi_gan(cursor, don_vi_gan):
    don_vi_gan = don_vi_gan.split()[0]
    query = """
    SELECT loaihinh
    FROM jwdb.app_fd_etl_company
    WHERE %s like company_code
    """
    cursor.execute(query, (don_vi_gan,))
    result = cursor.fetchall()
    return result

def get_email_by_jodcode(cursor, maticket, logging):
    ticket = get_ticket_by_maticket(cursor, maticket)
    logging.info(f"data: {ticket.get('c_phanLoai', '')} {ticket.get('c_NhomYeuCau', '')} {ticket.get('c_DanhMucYeuCau', '')}")
    L1, L2, L3 = fetch_data(cursor, ticket.get("c_phanLoai", ""), ticket.get("c_NhomYeuCau", ""), ticket.get("c_DanhMucYeuCau", "") )
    Nhom = ticket.get("c_individual", "")
    # donvigan = ticket.get("c_DonViGan", "")
    
    # get email sản phẩm dịch dụ
    if L1 == "Ngoài phạm vi":
        email = get_email_NPV(L1, L2, logging)
    
    elif L1 == "Sản phẩm dịch vụ":
        email = get_email_from_SPDV(L1, L2, L3, Nhom, logging)
        
    # get email rủi ro
    elif L1 == "Rủi ro" and L3 == "Rủi ro tuân thủ":
        email = get_email_RR(cursor,logging)
    
    # get email Tu van
    elif L1 == "Tư vấn" or L1 == "Đặt lịch hẹn":
        email = get_email_TV(Nhom, ticket.get("c_DonViGan", ""), cursor, logging)
    
    # get email theo
    else:
        email = get_from_L3(L3, ticket.get("c_DonViGan", ""), cursor, logging)
    return email
      # Trả về email nếu tìm thấy, h