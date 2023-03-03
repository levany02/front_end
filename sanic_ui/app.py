from sanic import Sanic
from sanic.response import text

app = Sanic("MyHelloWorldApp")

@app.get("/")
async def hello_world(request):
    return text("Hello, world.")


@app.get("/foo", strict_slashes=True)
@app.ext.template("foo.html")
async def handeler(request):
    return {"seq": ["one", "two"]}


@app.route("/search", methods=["GET"])
@app.ext.template("search_result.html")
def search_result(request):
    print(request.args)
    job_lst = [{"title": "Fresher Data Scientist", "location": "Vietnam", "level": "Fresher"}]
    return {"results":job_lst}


@app.route("/search-page")
@app.ext.template("form.html")
async def navi_search_page(request):
    return {}


@app.route("/events-page")
@app.ext.template("events.html")
async def navi_events_page(request):
    return {}


@app.route('/events')
def background_process_test(request):
    print ("Hello")
    return ("nothing")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, debug=True)