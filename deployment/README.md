# Virtual Machine Environment Setup

This project uses a pre-configured Virtual Machine (Ubuntu via VirtualBox) provided for the course. This document outlines how to connect to the VM and configure your local development environment.

## 1. Prerequisites

* **Oracle VirtualBox** installed.

* The course **`.ova` image** imported into VirtualBox.

* The VM status must be **Running**.

## 2. Port Forwarding Configuration

Configure the following rules in VirtualBox (**Settings -> Network -> Adapter 1 -> Advanced -> Port Forwarding**).

| Name | Protocol | Host IP | Host Port | Guest IP | Guest Port |
| :--- | :--- | :--- | :--- | :--- | :--- |
| ssh | TCP | 127.0.0.1 | 2222 | | |
| tcp16010 | TCP | | 16010 | | 16010 |
| tcp18080 | TCP | | 18080 | | 18080 |
| tcp18888 | TCP | | 18888 | | 18888 |
| tcp4040 | TCP | | 4040 | | 4040 |
| tcp50070 | TCP | | 50070 | | 50070 |
| tcp50075 | TCP | | 50075 | | 50075 |
| tcp8080 | TCP | | 8080 | | 8080 |
| tcp8081 | TCP | | 8081 | | 8081 |
| tcp8088 | TCP | | 8088 | | 8088 |
| tcp9083 | TCP | | 9083 | | 9083 |
| tcp9443 | TCP | | 9443 | | 9443 |

## 3. SSH Connection (Terminal)

The VM exposes the SSH service on the host's loopback interface via port **2222**.

To connect, you need the **private key**, available from labs.

### Connection Command

**Syntax:**

```bash

ssh -i "<PATH\_TO\_PRIVATE\_KEY>" vagrant@localhost -p 2222

```

## 4. Clone Github Repository

Inside the VM:

```bash

git clone https://github.com/Sebislaw/Crypto-Options-vs-Rates.git

```