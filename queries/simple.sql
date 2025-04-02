SELECT c_custkey, o_orderkey, c_name, o_totalprice
from orders,
    customer
where orders.o_custkey = c_custkey
order by c_custkey