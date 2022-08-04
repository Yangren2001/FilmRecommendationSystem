/*
登录事件
 */

var login = document.getElementById("login");
login.addEventListener("click", function () {
    var user = document.getElementsByName("user")[0].value;
    var pwd = document.getElementsByName("password")[0].value;
    $.post("/login", data={
        "user":user,
        "pwd":pwd
    }, function (data)
    {
        data = JSON.parse(data);
        if(data.login === true)
        {
            let a1 = document.createElement("a");
            a1.setAttribute("href", "/home.html");
            a1.click();
        }
        else
        {
            alert("账号或密码错误！");
        }

    });
});
var register = document.getElementById("register");

register.addEventListener("click", function () {
    let a1 = document.createElement("a");
    a1.setAttribute("href", "/register.html");
    a1.click();
})
