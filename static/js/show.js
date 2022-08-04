var parse_cookie = function () {
    let cookies = document.cookie;
    if(cookies === ""){
        return null;
    }
    else{
        let cookie_ = new Object();
        s = cookies.split(";")
        for(let i=0; i < s.length; i++){
            let c = s[i].split("=");
            try{
                let v = eval(c[1]);
                eval("cookie_." + c[0] + "=" + v);
            }
            catch (e){
                eval("cookie_." + c[0] + "= '" + c[1] + "'");
            }
        }
        return cookie_
    }
}


var m_show = function () {


    let base_url = location.href.split("/");
    let mid = base_url[base_url.length - 1];
    $.post("/page/" + mid,
        data={
        mid: mid
        },
        function f(data) {
           data = JSON.parse(data);
           console.log(data[0]);
           let m = document.getElementById("movies_show");
           m.setAttribute("class", "m2");
           let nr = data.data;
           if (data.state === true){
               let name = new Array(
                   "电影名称",
                   "时长",
                   "发行时间",
                   "拍摄时间",
                   "语言",
                   "详情描述",
                   "演员",
                   "导演",
                   "评分",
                   "类型"
               );
               let img = document.createElement("img");
               img.setAttribute("class", "img1");
                img.setAttribute("alt", "load....");
                img.setAttribute("src", "../image/" + nr[0] + "/picture.jpg");
                m.appendChild(img);
                for(let i=1; i < nr.length; i++){
                    let p = document.createElement("p");
                    let span = document.createElement("span");
                    let text = document.createTextNode(name[i - 1] +":");
                    let text1 = document.createTextNode(nr[i]);
                    p.setAttribute("class", "p_1");
                    span.setAttribute("class", "span_1");
                    span.appendChild(text);
                    p.append(span);
                    p.appendChild(text1);
                    m.append(p);
                }
           }
        }
        )

}

window.onload = function () {
    let cookies = parse_cookie();
    if(cookies === null){
        let a1 = document.createElement("a");
        a1.setAttribute("href", "/index.html");
        a1.click();
    }
    else {
        try{
        if(cookies.login === 0){
            let a1 = document.createElement("a");
            a1.setAttribute("href", "/index.html");
            a1.click();
        }}
        catch (e) {
            let a1 = document.createElement("a");
            a1.setAttribute("href", "/index.html");
            a1.click();
        }
        // a = JSON.parse(cookies);
        // console.log(cookies);
    }
    m_show();
    let base_url = location.href.split("/");
    let mid = base_url[base_url.length - 1];
    let m1 = document.getElementById("o");
    let m2 = document.getElementById("d");
    $.get(
                    "/rating/" + mid,
                    function (data) {
                        data = JSON.parse(data);
                        if(data.state === true){
                            m1.setAttribute("class", "m4");
                            m2.setAttribute("class", "");
                            let p = document.createElement("h3");
                            let text = document.createTextNode("您对当前电影的评分为:" + data.rating);
                            p.appendChild(text);
                            m2.append(p);
                        }
                        else {
                            m1.setAttribute("class", "");
                            m2.setAttribute("class", "m4");
                        }

        }
            );
    let rating_bt = document.getElementById("rating_bt");
    rating_bt.addEventListener("click", function () {
        let data = document.getElementsByName("rating")[0].value;
        if(data === ""){
            alert("评分不能为空")
        }
        else if(eval(data) < 0 || eval(data) > 5){
            alert("评分范围为0-5之间");
        }
        else {
            $.post(
                "/rating/" + mid,
                data={
                    "rating": data
                },
                function (data) {
                    data = JSON.parse(data);
                    if(data.state === true){
                        alert("评分成功");
                        location.reload();
                    }
                    else {
                         alert("由于未知原因评分未成功！非常抱歉");
                    }
                }
            )
        }
    })



}