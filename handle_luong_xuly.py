from SLA.handle_manual_email import handel_email
from xuly_luong_ticket_item.xuly_luong_ticket_its_ho import handler_its_ho


def update_status(cursor, ticket_id, status):
    query = """
            UPDATE jwdb.app_fd_sp_tickets
            SET c_trangThai = %s,
                c_field2 = '1'
            WHERE id = %s
            """
    cursor.execute(query, (status,ticket_id))
    

def handle_manual_ticket(conn, cursor, ticket_id, status, sla, sla_xuly, tong_ngay, assignment, flag_email, api, rabbitmq_channel, obj, logging):
    try:
        logging.info("handle_manual_ticket")
        logging.info(f"status: {status}")
        if status == "Đóng hoặc Đã chuyển tiếp":

            logging.info(f"Không thực hiện cập nhật status cho Ticket {ticket_id} với status 'Đóng hoặc Đã chuyển tiếp'.")
            return
        if status == "Đóng":
            update_status(cursor, ticket_id, status)
            logging.info(f"Status của Ticket {ticket_id} đã được chuyển thành 'Đóng'.")
        if status == "Mở":

            update_status(cursor, ticket_id, status)
        elif status == "Đã chuyển tiếp/Đóng":
            if flag_email:

                update_status(cursor, ticket_id,  "Đã chuyển tiếp")
            else:

                update_status(cursor, ticket_id,  "Đóng")

        else:
            if obj == "API":
                handler_its_ho(assignment, api, status, ticket_id, rabbitmq_channel, logging, conn)
                if status == "Đóng":
                    update_status(cursor, ticket_id, status)
                else:
                    update_status(cursor, ticket_id, "Đã chuyển tiếp")

            else:
                update_status(cursor, ticket_id, status)
                
    except Exception as e:
        logging.error(f"Error: {e}")


def handle_automatic_ticket(conn, jobcode, createdBy, email, cursor, ticket_id, status, status_ticket, sla, sla_xuly, tong_ngay, assignment, api, rabbitmq_channel, obj, list_email, sla_phanHoi, logging):
    try:
        if obj == "API":
            handler_its_ho(assignment, api, status, ticket_id, rabbitmq_channel, logging, conn)
            if status == "Đóng":
                    update_status(cursor, ticket_id, status)
            else:
                update_status(cursor, ticket_id, "Đã chuyển tiếp")

        elif obj == "Mail":
            try:
                if status_ticket != "Đóng":
                    logging.info("start send mail")
                    handel_email(cursor, email, jobcode, createdBy, assignment, ticket_id, list_email, sla_phanHoi)
                    update_status(cursor, ticket_id, status)

            except Exception as e:
                logging.error(f"Error send mail: {e}")
            
    except Exception as e:
        logging.error(f"Error: {e}")

    
