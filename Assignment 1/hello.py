from flask import Flask,request,send_file,Response,escape
from sqlitedict import SqliteDict
import flask_monitoringdashboard as dashboard
import json
import hashlib
import qrcode


app = Flask(__name__)
mydict = SqliteDict('./my_db.sqlite', autocommit=True)
dashboard.bind(app)




@app.route('/')
def hello():
    return {
        "haha":"haha"
    }  
    
# POST/api/bookmarks
@app.route('/api/bookmarks',methods=['POST'])
def create_bookmark():
    # after receving the request, parsing request data as json
    data = request.get_json()
    # get a unique id for each URL by doing hash
    hash_id = hashlib.sha224(str(data["url"]).encode('utf-8')).hexdigest()
    data["id"] = hash_id
    # check if the current id is already in the database
    for value in mydict.values():
        if data['url'] in value.values():
            # if already in database, return 400
            return {"reason": "The given URL already existed in the system."}, 400
    # confimred the id is not in database, then store it in database
    data['count'] = 0
    mydict[data["id"]] = data
    # mydict.close()
    # finished storing data in database, return the id to user
    return {"id": data["id"]}, 201




# GET /api/bookmarks/abc123
# DELETE /api/bookmarks/abc123
@app.route('/api/bookmarks/<id>',methods=['GET','DELETE'])
def handle_bookmark(id):
    if request.method == 'GET':
        if id in mydict:

            # retrieve id from db, and save it in res
            res = mydict[id]

            # increment count by 1
            res['count'] += 1
            mydict[id] = res

            # mydict.close()
            return res, 200
        else:
            return "Not found", 404
    elif request.method == 'DELETE':
        if id in mydict:
            # del dict[key]:Deletion: Remove key from dict
            del mydict[id]
            # mydict.close()
        return "No Content", 204
    

# GET /api/bookmarks/abc123/qrcode
# https://www.qr-code-generator.com/
@app.route('/api/bookmarks/<id>/qrcode',methods=['GET'])
def handle_qrcode(id):
    if id in mydict:
        # example data
        data = mydict[id]
        url = data["url"]
        # output file name
        filename = "bookmakr.png"
        # generate qr code
        img = qrcode.make(url)
        # save img to a file
        img.save(filename)
        return send_file(img,mimetype='image/gif'),200
    else:
        return "Not found", 404


# GET /api/bookmarks/{id}/stats
# https://pypi.org/project/flask-api-stats/

# # Custom Resource Class
# class Resource(RestplusResource):
#     def dispatch_request(self, *args, **kwargs):
#         resp = super(Resource, self).dispatch_request(*args, **kwargs)

#         # ETag checking.
#         # Check only for GET requests, for now.
#         if request.method == 'GET':
#             old_etag = request.headers.get('If-None-Match', '')
#             # Generate hash
#             data = json.dumps(resp)
#             new_etag = md5(data).hexdigest()

#             if new_etag == old_etag:
#                 # Resource has not changed
#                 return '', 304
#             else:
#                 # Resource has changed, send new ETag value
#                 return resp, 200, {'ETag': new_etag}

#         return resp

@app.route('/api/bookmarks/<id>/stats',methods=['GET'])
def stats(id):
    # read request header
    etag = request.headers.get('Etag')

    # # escape the id
    # id = escape(id)

    if id in mydict:

        data = mydict.get(id)

        #Etag header is of type string
        count = str(data['count'])

        if etag == count:
            # Resource has not changed
            return Response(headers={'Etag':count}, status=304)
        else:
            # Resource has changed, send new ETag value
            return Response(headers={'Etag':count}, status=200, response=count)
    else: 
        return {"reason": "Not Found"},404



