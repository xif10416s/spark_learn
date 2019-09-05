package org.fxi.test.spark.core.mock.communication.test.core


import org.fxi.test.spark.core.mock.communication.core.RpcEndpointRef


case class RegisterApplication(appDescription: String, driver: RpcEndpointRef)

case class RegisteredApplication(appId: String, master: RpcEndpointRef)