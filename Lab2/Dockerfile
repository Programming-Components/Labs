FROM nginx:alpine
ADD Tarasov.rar /usr/local/ 
# додавання у контейнер архіву та його автоматичне розархівування 
RUN apk update && apk add vim
ARG a_variable 
ENV env_variable=$a_variable 
#динамічне задання ENV-змінної, якщо вона не буде перевизначена, 
#то буде використане значення за замовчуванням
COPY nginx.conf /etc/nginx/nginx.conf
#COPY index.html /usr/share/nginx/html/index.html
WORKDIR /usr/share/nginx/html/
RUN echo "Hello $env_variable" > index.html
EXPOSE 8012
