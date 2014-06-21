DYNAMODB_LOCAL_VERSION = 2014-04-24
DYNAMODB_LOCAL_DIR = dynamodb_local

all: dynamodb_local_$(DYNAMODB_LOCAL_VERSION).tar.gz supervisord

dynamodb_local_$(DYNAMODB_LOCAL_VERSION).tar.gz: $(DYNAMODB_LOCAL_DIR)
	curl -O https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_$(DYNAMODB_LOCAL_VERSION).tar.gz
	tar -C $(DYNAMODB_LOCAL_DIR) -zxf dynamodb_local_$(DYNAMODB_LOCAL_VERSION).tar.gz

$(DYNAMODB_LOCAL_DIR):
	mkdir $(DYNAMODB_LOCAL_DIR)

supervisord: venv
	./venv/bin/supervisord

venv:
	virtualenv venv && . ./venv/bin/activate && pip install -r requirements.txt

stop:
	./venv/bin/supervisorctl stop all
	kill `cat supervisord.pid`

venvclean:
	rm -rf venv

dynamodblocalclean:
	rm -rf dynamodb_local

supervisordclean:
	rm -f supervisord.log

cleanall: venvclean dynamodblocalclean supervisordclean
