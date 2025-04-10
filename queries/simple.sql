SELECT c_custkey,
    o_orderkey,
    c_name,
    o_totalprice
from customer,
    orders
where orders.o_custkey = c_custkey
    and c_custkey < 1000
order by c_custkey