FROM node:10.12.0
USER node
RUN chmod 777 /nebulae/tpm/emi-gateway
COPY emi-gateway /nebulae/tpm/emi-gateway
WORKDIR /nebulae/tpm/emi-gateway
RUN npm install --prod
CMD [ "npm","start" ]
