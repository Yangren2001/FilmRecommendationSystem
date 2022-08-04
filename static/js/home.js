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


// 类别选择列表
var create_radio_ls = function () {

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
    let lb = document.getElementById("select");
     $.ajax(
         {
             url: "/genres",
             async: false,
             type: "GET",
             dataType: "json",
             success: function (data, status) {
                 // data = JSON.parse(data);

                 count = 0;
                 c1 = 0;
                 for (let item in data) {
                     let ipt = document.createElement("input");
                     // ipt.setAttribute("id", "i" + c1.toString());
                     ipt.setAttribute("type", "radio");
                     ipt.setAttribute("style", "margin-left: 18px");
                     ipt.setAttribute("value", data[item]);
                     ipt.setAttribute("name", "genres");
                     let text = document.createTextNode(item);
                     lb.append(ipt);
                     lb.appendChild(text);
                     count++;
                     c1++;
                     if (count === 6) {
                         count = 0;
                         lb.appendChild(document.createElement("br"));
                     }
                 }
             }
         }
    );

}

var GetData = function (v) {
    $.post(
                "/genres",
                data={
                    "genres": v
                },
                function (data) {
                    data = JSON.parse(data);
                    let  nr = data.data;
                    let d = document.getElementById("dialog");
                    d.close();
                    let s = document.getElementById("show");
                    for(let i=0; i < 2; i++) {
                        let div = document.createElement("div");
                        div.setAttribute("class", "div_show");
                        let ul = document.createElement("ul");
                        ul.setAttribute("class", "ul_show");

                        if (i === 0) {
                            let title = document.createElement("h2");
                            let t = document.createTextNode("离线推荐：");
                            title.appendChild(t);
                            div.append(title);
                            s.append(div);
                        }
                        else if (i === 1) {
                            let title = document.createElement("h2");
                            let t = document.createTextNode("实时推荐：");
                            title.appendChild(t);
                            div.append(title);
                            nr = data.real_data;
                            s.append(div);
                        }
                        if((i === 1 && data.real_data_state === false) ||( i === 0 && data.data_state === false) || (i===1 && nr.length===0)){
                             let con = document.createElement("h3");
                             let con_1 = document.createTextNode("暂时没有相关数据");
                             con.appendChild(con_1);
                             div.append(con);
                            continue;
                        }
                            for (let j = 0; j < nr.length; j++) {
                                let li = document.createElement("li");
                                li.setAttribute("class", "li_show");
                                let a = document.createElement("a");
                                let img = document.createElement("img");
                                a.setAttribute("href", "/page/" + nr[j][0]);
                                img.setAttribute("alt", "load....");
                                img.setAttribute("src", "../image/" + nr[j][0] + "/poster.jpg");
                                let p = document.createElement("p");
                                let t1 = document.createTextNode(nr[j][1]);
                                p.appendChild(t1);
                                a.appendChild(img);
                                a.append(p);
                                li.append(a);
                                ul.append(li);
                                // let p1 = document.createElement("img");
                                // let p2 = document.createElement("img");
                                // let p3 = document.createElement("img");
                                // let p4 = document.createElement("img");
                                // let p5 = document.createElement("img");
                                // let p6 = document.createElement("img");
                                // let p7 = document.createElement("img");
                                // let p8 = document.createElement("img");
                                // let p9 = document.createElement("img");

                            }
                            div.append(ul);

                            // console.log(i === 1);

                    }


                }
            );

}

// 提交数据
var st = function (){
    let ipt = document.getElementsByName("genres");
    for(let i=0; i < ipt.length; i++){
        if(ipt[i].checked === true){
            // console.log(ipt[i].value);

        }
    }
}

window.onload = function () {
    // create_radio_ls();
    GetData("");
    let bt = document.getElementById("search");
    bt.addEventListener("click", function () {
        let data = document.getElementsByName("search")[0].value;
        if(data === ""){
            alert("内容不能为空");
        }
        else {
            $.get("/search?search="+data,
                function (data) {
                data = JSON.parse(data);
                if(data.state === true){
                    let a1 = document.createElement("a");
                    a1.setAttribute("href", "/page/" + data.data);
                    a1.click();
                }
                else {
                    alert("暂时没有相关资源！");
                }
                });
        }

    })
}