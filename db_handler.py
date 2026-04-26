from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """

    cur.execute(
        "INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, "
        "i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned) "
        "VALUES ((SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item AS tmp), "
        "?, ?, ?, ?, NULL, ?, ?, ?, ?)",
        (new_item.item_id, f"{new_item.start_year}-01-01", new_item.product_name,
         new_item.brand, new_item.category, new_item.manufact, new_item.current_price, new_item.num_owned)
    )


    #raise NotImplementedError("you must implement this function")


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    address = new_customer.address.split(" ")
    cur.execute(
        "INSERT INTO customer_address "
        "(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) "
        "VALUES ((SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address AS tmp), "
        "?, ?, ?, ?, ?)",
        (address[0], address[1]+" "+address[2], address[3], address[4], address[5])
    )
    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    addr_sk = cur.fetchone()[0]

    name = new_customer.name.split(" ")
    cur.execute(
        "INSERT INTO customer "
        "(c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) "
        "VALUES ((SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer AS tmp), "
        "?, ?, ?, ?, ?)",
        (new_customer.customer_id, name[0], name[1], new_customer.email, addr_sk)
    )

    # raise NotImplementedError("you must implement this function")


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """

    if new_customer.name is not None:

        query = ("UPDATE customer SET customer.c_first_name = ?, customer.c_last_name = ? "
                 "WHERE c_customer_id = ?")
        cur.execute(query,
                    (new_customer.name.split(" ")[0], new_customer.name.split(" ")[1], original_customer_id))

        query = "SELECT * FROM customer WHERE c_customer_id = ?"
        cur.execute(query, (original_customer_id,))
        new_mail = cur.fetchone()
        print(new_mail)

    if new_customer.email is not None:

        query = "UPDATE customer SET customer.c_email_address = ? WHERE c_customer_id = ?"
        cur.execute(query, (new_customer.email, original_customer_id, ))

        query = "SELECT * FROM customer WHERE c_customer_id = ?"
        cur.execute(query, (original_customer_id, ))
        new_mail = cur.fetchone()
        print(new_mail)

    if new_customer.address is not None:

        new_address = new_customer.address.split(" ")
        cur.execute("SELECT c_current_addr_sk FROM customer WHERE c_customer_id = ?", (original_customer_id, ))
        old_addr = cur.fetchone()[0]

        query = ("UPDATE customer_address SET "
                 "customer_address.ca_street_number = ?, customer_address.ca_street_name = ?, "
                 "customer_address.ca_city = ?, customer_address.ca_state = ?, customer_address.ca_zip = ? "
                 "WHERE ca_address_sk = ?")
        cur.execute(query, (new_address[0], " ".join(new_address[1:-3]), new_address[-3],
                            new_address[-2], new_address[-1], old_addr))

        query = "SELECT * FROM customer WHERE c_customer_id = ?"
        cur.execute(query, (original_customer_id,))
        new_mail = cur.fetchone()
        print(new_mail)

    if new_customer.customer_id is not None:

        query = "UPDATE customer SET c_customer_id = ? WHERE c_customer_id = ?"
        cur.execute(query, (new_customer.customer_id, original_customer_id))

        query = "SELECT * FROM customer WHERE c_customer_id = ?"
        cur.execute(query, (new_customer.customer_id,))
        new_mail = cur.fetchone()
        print(new_mail)
    #raise NotImplementedError("you must implement this function")


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """

    rental_date = date.today()
    due_date = rental_date + timedelta(days=14)

    query = "INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)"
    cur.execute(query, (item_id, customer_id, str(rental_date), str(due_date)))

    #raise NotImplementedError("you must implement this function")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    cur.execute("(SELECT COALESCE(MAX(place_in_line), 0) + 1 FROM waitlist AS tmp WHERE item_id=?)", (item_id, ))
    pil = cur.fetchone()[0]

    query = "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)"
    cur.execute(query, (item_id, customer_id, pil))
    cur.execute("SELECT place_in_line FROM waitlist WHERE customer_id = ? AND item_id = ?",
                (customer_id,item_id))

    return int(cur.fetchone()[0])

    #raise NotImplementedError("you must implement this function")


def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """

    query = "DELETE FROM waitlist WHERE item_id = ? AND place_in_line = 1"
    cur.execute(query, (item_id,))

    query = "UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id = ?"
    cur.execute(query, (item_id,))

    #raise NotImplementedError("you must implement this function")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """

    query = "SELECT * FROM rental WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id,))
    rentals = list(cur.fetchone())

    query = "DELETE FROM rental WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id,))

    query = "INSERT INTO rental_history VALUES (?, ?, ?, ?, ?)"
    rentals.append(str(date.today()))
    cur.execute(query, tuple(rentals))


    #raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """

    query = "SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id,))
    ddate = date.fromisoformat(str(cur.fetchone()[0]))

    new_ddate = ddate + timedelta(days=14)

    query = "UPDATE rental SET due_date = ? WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (new_ddate, item_id, customer_id,))


    #raise NotImplementedError("you must implement this function")


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """

    query = ("SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact, "
             "i_current_price, YEAR(i_rec_start_date), i_num_owned FROM ITEM WHERE TRUE")
    questions = []

    if filter_attributes.item_id is not None:
        if use_patterns:
            query += " AND i_item_id LIKE ?"
        else:
            query += " AND i_item_id = ?"
        questions.append(filter_attributes.item_id)

    if filter_attributes.product_name is not None:
        if use_patterns:
            query += " AND i_product_name LIKE ?"
        else:
            query += " AND i_product_name = ?"
        questions.append(filter_attributes.product_name)

    if filter_attributes.brand is not None:
        if use_patterns:
            query += " AND i_brand LIKE ?"
        else:
            query += " AND i_brand = ?"
        questions.append(filter_attributes.brand)

    if filter_attributes.category is not None:
        if use_patterns:
            query += " AND i_category LIKE ?"
        else:
            query += " AND i_category = ?"
        questions.append(filter_attributes.category)

    if filter_attributes.manufact is not None:
        if use_patterns:
            query += " AND i_manufact LIKE ?"
        else:
            query += " AND i_manufact = ?"
        questions.append(filter_attributes.manufact)

    # start current_price checking

    if filter_attributes.current_price != -1:
        query += " AND i_current_price = ?"
        questions.append(filter_attributes.current_price)

    if min_price != -1:
        query += " AND i_current_price >= ?"
        questions.append(min_price)

    if max_price != -1:
        query += " AND i_current_price <= ?"
        questions.append(max_price)

    # stop current_price checking

    #begin start_year checking

    if filter_attributes.start_year != -1:
        query += " AND YEAR(i_rec_start_date) = ?"
        questions.append(filter_attributes.start_year)

    if min_start_year != -1:
        query += " AND YEAR(i_rec_start_year) >= ?"
        questions.append(min_start_year)

    if max_start_year != -1:
        query += " AND YEAR(i_rec_start_year) <= ?"
        questions.append(max_start_year)

    #stop start_year checking

    if filter_attributes.num_owned != -1:
        query += " AND i_num_owned = ?"
        questions.append(filter_attributes.num_owned)

    cur.execute(query, tuple(questions))
    items = []
    for i in cur.fetchall():
        items.append(Item(*i))

    return items


    #raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    query = ("SELECT c_customer_id, CONCAT(c_first_name,\" \",c_last_name), "
             "(SELECT CONCAT(ca_street_number, \" \", ca_street_name, \" \", ca_city,"
             "\" \", ca_state, \" \", ca_zip) "
             "FROM customer_address WHERE ca_address_sk=c_current_addr_sk)"
             ",c_email_address FROM customer WHERE TRUE")
    questions = []

    if use_patterns:
        if filter_attributes.customer_id is not None:
            query += " AND c_customer_id LIKE ?"
            questions.append(filter_attributes.customer_id)
        if filter_attributes.name is not None:
            nm = filter_attributes.name.split(" ")
            query += " AND c_first_name LIKE ? AND c_last_name LIKE ?"
            questions.append(nm[0])
            questions.append(nm[1])
        if filter_attributes.email is not None:
            query += " AND c_email_address LIKE ?"
            questions.append(filter_attributes.email)
        if filter_attributes.address is not None:
            addr = filter_attributes.address.split(" ")
            query += " AND c_current_addr_sk = ?"
            cur.execute("SELECT ca_address_sk FROM customer_address WHERE ca_street_number LIKE ? "
                        "AND ca_street_name LIKE ? AND ca_city LIKE ? AND ca_state LIKE ? AND ca_zip LIKE ?",
                        (addr[0], " ".join(addr[1:-3]), addr[-3], addr[-2], addr[-1]))
            res = cur.fetchone()
            if res is not None:
                questions.append(res[0])
            else:
                return []
    else:
        if filter_attributes.customer_id is not None:
            query += " AND c_customer_id = ?"
            questions.append(filter_attributes.customer_id)
        if filter_attributes.name is not None:
            nm = filter_attributes.name.split(" ")
            query += " AND c_first_name = ? AND c_last_name = ?"
            questions.append(nm[0])
            questions.append(nm[1])
        if filter_attributes.email is not None:
            query += " AND c_email_address = ?"
            questions.append(filter_attributes.email)
        if filter_attributes.address is not None:
            addr = filter_attributes.address.split(" ")
            query += " AND c_current_addr_sk = ?"
            cur.execute("SELECT ca_address_sk FROM customer_address WHERE ca_street_number = ? "
                        "AND ca_street_name = ? AND ca_city = ? AND ca_state = ? AND ca_zip = ?",
                        (addr[0], " ".join(addr[1:-3]), addr[-3], addr[-2], addr[-1]))
            res = cur.fetchone()
            if res is not None:
                questions.append(res[0])
            else:
                return []

    cur.execute(query, tuple(questions))
    customers = []
    for i in cur.fetchall():
        print(i)
        customers.append(Customer(*i))

    return customers
    # raise NotImplementedError("you must implement this function")


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    query = "SELECT item_id, customer_id, rental_date, due_date FROM rental WHERE TRUE"
    questions = []

    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        questions.append(filter_attributes.customer_id)
    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        questions.append(filter_attributes.item_id)
    if filter_attributes.rental_date is not None:
        query += " AND rental_date = ?"
        questions.append(filter_attributes.rental_date)
    if filter_attributes.due_date is not None:
        query += " AND due_date = ?"
        questions.append(filter_attributes.due_date)

    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        questions.append(min_rental_date)
    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        questions.append(max_rental_date)
    if min_due_date is not None:
        query += " AND due_date >= ?"
        questions.append(min_due_date)
    if max_due_date is not None:
        query += " AND due_date <= ?"
        questions.append(max_due_date)

    cur.execute(query, tuple(questions))
    rentals = []
    for i in cur.fetchall():
        print(i)
        rentals.append(Rental(*i))

    return rentals
    #raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    query = "SELECT item_id, customer_id, rental_date, due_date, return_date FROM ITEM WHERE TRUE"
    questions = []

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        questions.append(filter_attributes.item_id)

    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        questions.append(filter_attributes.customer_id)


    if filter_attributes.rental_date is not None:
        query += " AND rental_date = ?"
        questions.append(filter_attributes.rental_date)

    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        questions.append(min_rental_date)

    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        questions.append(max_rental_date)


    if filter_attributes.due_date is not None:
        query += " AND due_date = ?"
        questions.append(filter_attributes.due_date)

    if min_due_date is not None:
        query += " AND due_date >= ?"
        questions.append(min_due_date)

    if max_due_date is not None:
        query += " AND due_date <= ?"
        questions.append(max_due_date)


    if filter_attributes.return_date is not None:
        query += " AND return_date = ?"
        questions.append(filter_attributes.return_date)

    if min_return_date is not None:
        query += " AND return_date >= ?"
        questions.append(min_return_date)

    if max_return_date is not None:
        query += " AND return_date <= ?"
        questions.append(max_return_date)


    cur.execute(query, tuple(questions))
    histories = []
    for i in cur.fetchall():
        histories.append(RentalHistory(*i))

    return histories

    #raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    query = "SELECT item_id, customer_id, place_in_line FROM waitlist WHERE TRUE"
    questions = []


    cur.execute(query, tuple(questions))
    waitlists = []
    for i in cur.fetchall():
        print(i)
        waitlists.append(Waitlist(*i))

    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """

    query = "SELECT COUNT(*) FROM rental WHERE item_id = ?"
    cur.execute(query, (item_id,))
    active_rentals = int(cur.fetchone()[0])

    query = "SELECT i_num_owned FROM item WHERE i_item_id = ?"
    cur.execute(query, (item_id,))
    num_owned = cur.fetchone()

    if len(num_owned) == 0:
        return -1
    else:
        return int(num_owned[0]) - active_rentals


    #raise NotImplementedError("you must implement this function")


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """

    query = "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id,))
    place = cur.fetchone()

    if place is None:
        return -1
    else:
        return int(place[0])

    #raise NotImplementedError("you must implement this function")


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """

    query = "SELECT COUNT(*) FROM waitlist WHERE item_id = ?"
    cur.execute(query, (item_id,))
    waiters = cur.fetchone()[0]

    return waiters

    #raise NotImplementedError("you must implement this function")


def save_changes():
    """
    Commits all changes made to the db.
    """

    conn.commit()

    #raise NotImplementedError("you must implement this function")


def close_connection():
    """
    Closes the cursor and connection.
    """

    if cur:
        cur.close()
    if conn:
        conn.close()

    #raise NotImplementedError("you must implement this function")

