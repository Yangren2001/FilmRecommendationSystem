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

var create_bt = function () {
    let bt_div = document.getElementById("bt");
    $.get("/genres",
        function (data) {
            data = JSON.parse(data);
            for(let item in data){
                let bt = document.createElement("a");
                bt.setAttribute("class", "a1");
                bt.setAttribute("href", "javascript:void(0);");
                bt.setAttribute("id", data[item]);
                let text = document.createTextNode(item);
                bt.appendChild(text);
                bt_div.append(bt);
                bt.addEventListener("click", function () {
                    // console.log(data[item]);
                    $.post("/movie_genres/" + data[item], function (data) {
                            data = JSON.parse(data);
                            let nr = data.data;
                             let s = document.getElementById("show");
                             let s1 = document.getElementById("div_show");
                             if(s1 !== null){
                                 s.removeChild(s1);
                             }
                            let div = document.createElement("div");
                            div.setAttribute("id", "div_show")
                            div.setAttribute("class", "div_show");
                            s.append(div);
                            let ul = document.createElement("ul");
                            ul.setAttribute("class", "ul_show");
                            if(data.state === false){
                                let p1 = document.createElement("h1");
                                let text0 = document.createTextNode("暂时没有相关数据！");
                                p1.appendChild(text0);
                                div.append(p1);
                            }
                            else {
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

                                }
                                div.append(ul);
                            }
                            })

                });
                if(data[item] === "Action"){
                    bt.click();
                }
            }
        })
}




window.onload = function (){
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
    create_bt();

}