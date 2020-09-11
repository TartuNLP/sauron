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
           "text":["Tere", "Nägemist"]
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

API requires two configuration files for execution:

__config.json__ contains a list of domains with their names and a corresponding list of translation engines that share the same set of supported factors like output style and language. Each of these workers has its name and two settings: IP address and port.

__dev.ini__ specifies a mapping from the authentification key to the domain in which translation is expected. In the example, the second line says that users with authentification key *public* will be directed to the general domain.
 
 Examples of __config.json__ and __dev.ini__ are provided in this repository.  
  
 Running from command line: 
 
 Unix Bash  
 `$ export FLASK_APP=sauron`  
 `$ flask run` 
 
 Windows CMD  
 `> set FLASK_APP=sauron`  
`> flask run`

#### Deploying: 

One way to deploy is [gunicorn](https://gunicorn.org/) and [GNU screen ](https://www.gnu.org/software/screen/manual/screen.html#Overview) session. To support start, stop and restart commands one can add these lines to the .bash_aliases:

```
alias sauron-start='cd project/directory && screen -S sauron sudo gunicorn3 [OPTIONS] && cd ~'
alias sauron-stop='ps aux |grep gunicorn |grep sauron | awk '"'"'{ print $2 }'"'"' | sudo xargs kill -s QUIT'
alias sauron-restart='sauron-stop && sauron-start'

```

 
