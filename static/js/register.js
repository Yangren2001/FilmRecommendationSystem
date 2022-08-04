var r = document.getElementById("return");

r.addEventListener("click", function () {
    let a1 = document.createElement("a");
    a1.setAttribute("href", "/index.html");
    a1.click();
})

var register = document.getElementById("register");

register.addEventListener("click", function () {
    let u = document.getElementsByName("user_name")[0].value;
    let p = document.getElementsByName("password")[0].value;
    let n = document.getElementsByName("new_password")[0].value;
    let o = document.getElementsByName("occupation")[0].value;
    let s = document.getElementsByName("sex")[0].value;
    let a = document.getElementsByName("age")[0].value;
    if(u === "" || p === "" || n === "" || o === "" ||s === "" || a === ""){
        alert("注册信息不能为空")
    }
    else if(p !== n){
        alert("输入密码不一致！");

    }
    else {
        $.post("/register", data={
            "user_name":u,
            "password":p,
            "occupation":o,
            "sex":s,
            "age":a
        }, function (data) {
            data = JSON.parse(data);
            if(data.register_state === true){
                alert(decodeURI(data.register_info));
                r.click();
            }
            else {
                // console.log(data.register_info);
                // console.log(decodeURI(data.register_info));
                alert(decodeURI(data.register_info));
            }
        });
    }

})