--Task 1: Identify Authors with the Most Published Books
with BooksPerAuthorCTE as (
    select a.name, count(*) as total
	from books b
	join authors a
	on b.author_id=a.author_id
	group by a.name
)
select * 
from BooksPerAuthorCTE
where total>3;

--Task 2: Identify Books with Titles Containing 'The' Using Regular Expressions
with the_books as (
    select title
    from books
    where lower(title) like '%the%'
)
select b.title, a.name as author, g.genre_name, b.published_date
from the_books tb
join books b on tb.title = b.title
join authors a on b.author_id = a.author_id
join genres g on b.genre_id = g.genre_id;

--Task 3: Rank Books by Price within Each Genre Using the RANK() Window Function
select g.genre_name, b.title, rank() 
	over (partition by b.genre_id order by b.price desc) as price_rank, b.price 
from books b
join genres g
on b.genre_id = g.genre_id
order by g.genre_name, price_rank;

--Task 4: Bulk Update Book Prices by Genre
create or replace procedure sp_bulk_update_book_prices_by_genre(
    p_genre_id integer,
    p_percentage_change numeric(5, 2)
)
language plpgsql
as $$
declare
    updated_count integer;
begin
    update books
    set price = price * (1 + p_percentage_change / 100)
    where genre_id = p_genre_id;

    get diagnostics updated_count = row_count;

    raise notice 'Number of books updated: %', updated_count;
end;
$$;

call sp_bulk_update_book_prices_by_genre(3, 5.00);

--Task 5: Update Customer Join Date Based on First Purchase
create or replace procedure sp_update_customer_join_date()
language plpgsql
as $$
declare
    updated_count integer;
begin
    update customers
    set join_date = subquery.first_purchase_date
    from (
        select customer_id, min(sale_date) as first_purchase_date
        from sales
        group by customer_id
    ) subquery
    where customers.customer_id = subquery.customer_id
      and subquery.first_purchase_date < customers.join_date;

	get diagnostics updated_count = row_count;
    raise notice 'Number of customers updated: %', updated_count;
end;
$$;

call sp_update_customer_join_date();

--Task 6: Calculate Average Book Price by Genre
create or replace function fn_avg_price_by_genre(p_genre_id integer)
returns numeric(10, 2)
language plpgsql
as $$
declare
    avg_price numeric(10, 2);
begin
    select avg(price) into avg_price
    from books
    where genre_id = p_genre_id;

    return avg_price;
end;
$$;

select fn_avg_price_by_genre(3);

--Task 7: Get Top N Best-Selling Books by Genre
create or replace function fn_get_top_n_books_by_genre(
    p_genre_id integer,
    p_top_n integer
)
returns table(
    title varchar,
    total_revenue numeric(10, 2)
)
language plpgsql
as $$
begin
    return query
    select b.title, sum(s.quantity * b.price) as total_revenue
    from books b
    join sales s on b.book_id = s.book_id
    where b.genre_id = p_genre_id
    group by b.title
    order by total_revenue desc
    limit p_top_n;
end;
$$;

select * from fn_get_top_n_books_by_genre(1, 5);

--Task 8: Log Changes to Sensitive Data
create table customerslog (
    log_id serial primary key,
    column_name varchar(50),
    old_value text,
    new_value text,
    changed_at timestamp default current_timestamp,
    changed_by varchar(50)
);

create or replace function tr_log_sensitive_data_changes_fn()
returns trigger
language plpgsql
as $$
begin
    if old.first_name is distinct from new.first_name then
        insert into customerslog (column_name, old_value, new_value, changed_by)
        values ('first_name', old.first_name, new.first_name, current_user);
    end if;
    
    if old.last_name is distinct from new.last_name then
        insert into customerslog (column_name, old_value, new_value, changed_by)
        values ('last_name', old.last_name, new.last_name, current_user);
    end if;

    if old.email is distinct from new.email then
        insert into customerslog (column_name, old_value, new_value, changed_by)
        values ('email', old.email, new.email, current_user);
    end if;

    return new;
end;
$$;

drop trigger if exists tr_log_sensitive_data_changes on customers;

create trigger tr_log_sensitive_data_changes
after update on customers
for each row
execute function tr_log_sensitive_data_changes_fn();

--Task 9: Automatically Adjust Book Prices Based on Sales Volume
create or replace function tr_adjust_book_price_fn()
returns trigger
language plpgsql
as $$
declare
    total_quantity integer;
    threshold integer := 10; 
begin
    select sum(quantity) into total_quantity
    from sales
    where book_id = new.book_id;

    if total_quantity >= threshold then
        update books
        set price = price * 1.10
        where book_id = new.book_id;
    end if;

    return new;
end;
$$;

drop trigger if exists tr_adjust_book_price on sales;

create trigger tr_adjust_book_price
after insert on sales
for each row
execute function tr_adjust_book_price_fn();

--Task 10: Archive Old Sales Records
create table salesArchive (
    sale_id serial primary key,
    book_id integer not null,
    customer_id integer not null,
    quantity integer not null,
    sale_date date not null,
    foreign key (book_id) references Books(book_id),
    foreign key (customer_id) references Customers(customer_id)
);

create or replace procedure sp_archive_old_sales(p_cutoff_date date)
language plpgsql
as $$
declare
    old_sales_cursor cursor for
		select * from sales
        where sale_date < p_cutoff_date;
	old_sale_record record;
begin
	open old_sales_cursor;
	loop
        fetch next from old_sales_cursor into old_sale_record;
        exit when not found;

		insert into salesArchive (book_id, customer_id, quantity, sale_date) 
		values (old_sale_record.book_id, old_sale_record.customer_id, old_sale_record.quantity, old_sale_record.sale_date);

		delete from sales where sale_id = old_sale_record.sale_id;
		
    end loop;
    close old_sales_cursor;
end; $$;

call sp_archive_old_sales('2023-07-03'::date);
