FROM teddysun/xray:latest

# 拷贝配置
COPY config.json /etc/xray/config.json

# 用脚本启动，替换 PORT_ENV
CMD sed -i "s/PORT_ENV/${PORT}/g" /etc/xray/config.json && \
    xray -config /etc/xray/config.json
