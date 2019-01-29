package com.example;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Random;

public class Main {

    static final Integer DEFAULT_PORT = 9090;
    public static void main(String[] args) throws IOException {
        Integer port;
        try {
            port = Integer.parseInt(args[0]);
        } catch(ArrayIndexOutOfBoundsException | NumberFormatException exc) {
            System.out.printf("Input parameter for port number wasn't provided or was not a valid number. Using default value: " + DEFAULT_PORT);
            port = DEFAULT_PORT;
        }

        ServerSocket listener = new ServerSocket(port);
        try {
            Socket socket = listener.accept();
            System.out.printf("New connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random random = new Random();
                while(true) {
                    int i = random.nextInt(100);
                    String s = Timestamp.from(Instant.now()) + "," + i;
                    System.out.println("Sending: " + s);
                    out.println(s);
                    Thread.sleep(100);
                }
            } finally {
                socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            listener.close();
        }
    }
}
