from asyncio import ThreadedChildWatcher

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

    if new_customer.customer_id is not None:

        query = "UPDATE customer SET customer.c_customer_id = ? WHERE c_customer_id = ?"
        cur.execute(query, (new_customer.customer_id, original_customer_id))

    if new_customer.name is not None:

        query = ("UPDATE customer SET customer.c_first_name = ?, customer.c_last_name = ? "
                 "WHERE c_customer_id = ?")
        cur.execute(query,
                    (new_customer.name.split(" ")[0], new_customer.name.split(" ")[1], original_customer_id))

    if new_customer.email is not None:

        query = "UPDATE customer SET customer.c_email_address = ? WHERE c_customer_id = ?"
        cur.execute(query, (new_customer.email, original_customer_id))

    if new_customer.address is not None:

        new_address = new_customer.address.split(" ")
        cur.execute("SELECT c_current_addr_sk FROM customer WHERE c_customer_id = ?", (original_customer_id, ))
        old_addr = cur.fetchone()[0]

        query = ("UPDATE customer_address SET "
                 "customer_address.ca_street_number = ?, customer_address.ca_street_name = ?, "
                 "customer_address.ca_city = ?, customer_address.ca_state = ?, customer_address.ca_zip = ? "
                 "WHERE ca_address_sk = ?")
        cur.execute(query, (new_address[0], new_address[1]+" "+new_address[2], new_address[3],
                            new_address[4], new_address[5], old_addr))

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

    cur.execute("SELECT place_in_line FROM waitlist WHERE customer_id = ? AND item_id",
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
    rentals = cur.fetchone()

    query = "DELETE FROM rental WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id,))

    query = "INSERT INTO rental_history VALUES (?, ?, ?, ?, ?)"
    rentals.append(str(date.today()))
    cur.execute(query, rentals)


    #raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """

    query = "SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id,))
    ddate = date.fromisoformat(cur.fetchone()[0])

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

    table_list = []

    if filter_attributes.item_id is not None:
        if use_patterns:
            query = "SELECT * FROM item WHERE i_item_id LIKE ?"
        else:
            query = "SELECT * FROM item WHERE i_item_id = ?"
        cur.execute(query, (filter_attributes.item_id,))
        ids = cur.fetchall()
        table_list.append(set(ids))

    if filter_attributes.product_name is not None:
        if use_patterns:
            query = "SELECT * FROM item WHERE i_product_name LIKE ?"
        else:
            query = "SELECT * FROM item WHERE i_product_name = ?"

        cur.execute(query, (filter_attributes.product_name,))
        names = cur.fetchall()
        table_list.append(set(names))

    if filter_attributes.brand is not None:
        if use_patterns:
            query = "SELECT * FROM item WHERE i_brand LIKE ?"
        else:
            query = "SELECT * FROM item WHERE i_brand = ?"
        cur.execute(query, (filter_attributes.brand,))
        brands = cur.fetchall()
        table_list.append(set(brands))

    if filter_attributes.category is not None:
        if use_patterns:
            query = "SELECT * FROM item WHERE i_category LIKE ?"
        else:
            query = "SELECT * FROM item WHERE i_category = ?"
        cur.execute(query, (filter_attributes.category,))
        categories = cur.fetchall()
        table_list.append(set(categories))

    if filter_attributes.manufact is not None:
        if use_patterns:
            query = "SELECT * FROM item WHERE i_manufact LIKE ?"
        else:
            query = "SELECT * FROM item WHERE i_manufact = ?"
        cur.execute(query, (filter_attributes.manufact,))
        manufacts = cur.fetchall()
        table_list.append(set(manufacts))

    # start current_price checking

    if filter_attributes.current_price != -1:

        query = "SELECT * FROM item WHERE i_current_price = ?"
        cur.execute(query, (filter_attributes.current_price,))
        prices = cur.fetchall()
        table_list.append(set(prices))


    if min_price != -1 and max_price != -1:
        query = "SELECT * FROM item WHERE i_current_price >= ? AND i_current_price <= ?"
        cur.execute(query, (min_price, max_price))
        prices = cur.fetchall()
        table_list.append(set(prices))

    elif min_price != -1:
        query = "SELECT * FROM item WHERE i_current_price >= ?"
        cur.execute(query, (min_price, ))
        prices = cur.fetchall()
        table_list.append(set(prices))

    elif max_price != -1:
        query = "SELECT * FROM item WHERE i_current_price <= ?"
        cur.execute(query, (max_price, ))
        prices = cur.fetchall()
        table_list.append(set(prices))

    # stop current_price checking

    #begin start_year checking

    if filter_attributes.start_year != -1:
        query = "SELECT * FROM item WHERE YEAR(i_rec_start_date) = ?"
        cur.execute(query, (filter_attributes.start_year,))
        year = cur.fetchall()
        table_list.append(set(year))

    if min_start_year != -1 and max_start_year != -1:
        query = ("SELECT * FROM item WHERE YEAR(i_rec_start_year) >= ? "
                 "AND YEAR(i_rec_start_year) <= ?")
        cur.execute(query, (min_start_year, max_start_year))
        years = cur.fetchall()
        table_list.append(set(years))
    elif min_start_year != -1:
        query = "SELECT * FROM item WHERE YEAR(i_rec_start_year) >= ?"
        cur.execute(query, (min_start_year,))
        years = cur.fetchall()
        table_list.append(set(years))
    elif max_start_year != -1:
        query = "SELECT * FROM item WHERE YEAR(i_rec_start_year) <= ?"
        cur.execute(query, (max_start_year,))
        years = cur.fetchall()
        table_list.append(set(years))

    #stop start_year checking

    if filter_attributes.num_owned != -1:
        query = "SELECT * FROM item WHERE i_num_owned = ?"
        cur.execute(query, (filter_attributes.num_owned,))
        owned = cur.fetchall()
        table_list.append(set(owned))




    mega_table = set.intersection(*table_list)

    return list(mega_table)


    #raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


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
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """

    query = "SELECT COUNT(*) FROM rental WHERE item_id = ?"
    cur.execute(query, (item_id,))
    active_rentals = int(cur.fetchone()[0])

    query = "SELECT i_num_owned FROM item WHERE item_id = ?"
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

    if len(place) == 0:
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

