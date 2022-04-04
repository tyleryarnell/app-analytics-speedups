all: build deploy

build:
	@echo "SAM build for deployment..."
	rm lib.zip || true
	zip -r lib.zip lib
	sam build --use-container --profile dev-proto --region us-west-2

deploy:
	@echo "SAM deploys..."
	sam deploy --no-confirm-changeset || true
	aws s3 cp scripts s3://app-analytics-resources --recursive --profile dev-proto 
	aws s3 cp lib.zip s3://app-analytics-resources/extra-py-files/ --profile dev-proto

validate:
	sam validate --profile dev-proto --region us-west-2

delete:
	sam delete
