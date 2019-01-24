FROM node:10.12.0
COPY emi-gateway /nebulae/tpm/emi-gateway
WORKDIR /nebulae/tpm/emi-gateway
RUN npm install --prod
USER node
CMD [ "npm","start" ]
