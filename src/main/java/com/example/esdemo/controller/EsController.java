package com.example.esdemo.controller;

import com.example.esdemo.document.Flight;
import com.example.esdemo.document.ProfileDocument;
import com.example.esdemo.service.EsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class EsController {
    @Autowired
    private EsService service;

    /**
     * 新增一条信息
     * @param document
     * @return
     * @throws Exception
     */
    @PostMapping
    public ResponseEntity createProfile(@RequestBody ProfileDocument document) throws Exception {
        return new ResponseEntity(service.createProfileDocument(document), HttpStatus.CREATED);
    }

    /**
     * 修改一条信息
     * @param document
     * @return
     * @throws Exception
     */
    @PutMapping
    public ResponseEntity updateProfile(@RequestBody ProfileDocument document) throws Exception {
        return new ResponseEntity(service.updateProfie(document), HttpStatus.CREATED);
    }

    /**
     * 根据文档id查询一条document
     * @param id
     * @return
     * @throws Exception
     */
    @GetMapping("/{id}")
    public ProfileDocument findById(@PathVariable String id) throws Exception {
        System.out.println("id是："+id);
        return service.findById(id);
    }

    /**
     * 查询所有的文档
     * @return
     * @throws Exception
     */
    @GetMapping
    public List<Flight> findAll() throws Exception {
        System.out.println("进入查询所有");
        return service.findAll();
    }


    /**
     * 根据名字查询
     * @param name
     * @return
     * @throws Exception
     */
    @GetMapping(value = "/api/v1/profiles/name-search")
    public List<Flight> searchByName(@RequestParam(value = "name") String name) throws Exception {
        return service.findProfileByName(name);
    }

    /**
     * 根据文档id 删除
     * @param id
     * @return
     * @throws Exception
     */
    @DeleteMapping("/{id}")
    public String deleteProfileDocument(@PathVariable String id) throws Exception {
        return service.deleteProfileDocument(id);
    }
}
