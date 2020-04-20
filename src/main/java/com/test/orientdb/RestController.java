package com.test.orientdb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;

@org.springframework.web.bind.annotation.RestController
public class RestController {

    @Autowired
    OrientDBService odbService;

    @PostMapping("/doSomeFancyStuff")
    public ResponseEntity doSomeFancyStuff() {

        odbService.doSomeFancyStuff();
        return new ResponseEntity(HttpStatus.NO_CONTENT);
    }
}
