# sauron
Open-source Neural MT server API

### Usage:

Request parameters :


 - auth
 - odomain
 - olang
 

POST http://translate.cloud.ut.ee/v1.2/translate?auth=public&olang=eng&odomain=auto  
BODY (JSON):

    {
       "text":"Tere"
    }


RESPONSE (JSON):

    {
        "status":"done",
        "input":"Tere",
        "result":"Hello"
    }


#### Translation (multiple sentences):

POST https://translate.cloud.ut.ee/v1.2/translate?auth=public&olang=eng&odomain=auto  
BODY (JSON):

        {
           "sentences":["Tere", "Nägemist"]
        }


RESPONSE (JSON):

        {
            "status":"done",
            "input":["Tere", "Nägemist"],
            "result":["Hello", "Bye"]
        }


#### Get help on domain settings:  

GET/ POST http://translate.cloud.ut.ee/v1.2/translate/support?auth=public

```json
{"domain":"general","options":[{"odomain":"fml","name":"Formal","lang":["est","lav","lit","ger","eng","fin","rus"]},{"odomain":"inf","name":"Informal","lang":["est","lav","lit","ger","eng","fin","rus"]},{"odomain":"auto","name":"Auto","lang":["est","lav","lit","ger","eng","fin","rus"]}]}
```
#### Errors  

You may also receieve a message. For example when incorrect usage is found:  

POST http://translate.cloud.ut.ee/v1.2/translate?auth=public

```json
{
    "message": "olang not found in request"
}
```

#### Requirements:

`pip3 install flask nltk`

#### Configuration:

 API requires configuration files for execution. Examples of __config.json__ and __dev.ini__ are provided in this repository.  
  
 Running from command line: 
 
 Unix Bash  
 `$ export FLASK_APP=api`  
 `$ flask run` 
 
 Windows CMD  
 `> set FLASK_APP=api`  
`> flask run`
 

 
