from typing import List, Dict, Any
from db import get_db_connection

def get_customers_by_credit_limit_range():
    """Return customers with credit limits outside the normal range."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "select customerName, creditLimit from customers where creditLimit < 10000 or creditLimit > 100000"
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()



    return res

def get_orders_with_null_comments():
    """Return orders that have null comments."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "select orderNumber, comments from orders where comments is null order by orderDate"
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res


def get_first_5_customers():
    """Return the first 5 customers."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "select customerName, contactLastName, contactFirstName from customers order by contactLastName limit 5"
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res


def get_payments_total_and_average():
    """Return total and average payment amounts."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "select sum(amount) as total_amount, avg(amount) as avg_total, min(amount), max(amount) from payments"
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res


def get_employees_with_office_phone():
    """Return employees with their office phone numbers."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = ("""select e.firstName, e.lastName, o.phone 
             from employees e
             join offices o
             on e.officeCode = o.officeCode
             """)
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res

def get_customers_with_shipping_dates():
    """Return customers with their order shipping dates."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """select c.customerName, o.shippedDate 
                from customers c
                left join orders o
                on c.customerNumber = o.customerNumber
                group by c.customerName, o.shippedDate
                """

    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res

def get_customer_quantity_per_order():
    """Return customer name and quantity for each order."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """select c.customerName, d.quantityOrdered
                    from customers c
                    join orders o
                    on c.customerNumber = o.customerNumber
                    join orderdetails d
                    on o.orderNumber = d.orderNumber
                    order by c.customerName
                    """
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res

def get_customers_payments_by_lastname_pattern(pattern: str = "son"):
    """Return customers and payments for last names matching pattern."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """select c.customerName, concat(c.contactFirstName, ' ',c.contactLastName) as contact_name, sum(amount) as total_amount 
                from customers c
                join payments p
                on c.customerNumber = p.customerNumber
                where c.contactFirstName like '%Mu%' or c.contactFirstName like '%Iy%' 
                group by c.customerName, c.contactFirstName, c.contactLastName
                order by total_amount desc
                """
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    cursor.close()
    return res