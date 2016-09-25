
namespace go thriftproxy_test

struct AddRequest {
1: i32 first,
2: i32 second,
}
struct AddResponse {
1: i32 sum,
}

service Adder{
   AddResponse add(1:AddRequest req),
}
