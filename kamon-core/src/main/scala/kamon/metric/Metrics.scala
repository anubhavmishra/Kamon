/*
 * =========================================================================================
 * Copyright © 2013 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.metric

import com.typesafe.config.Config
import kamon.metric.SubscriptionsDispatcher.{ Unsubscribe, Subscribe }
import kamon.metric.instrument.Gauge.CurrentValueCollector
import kamon.metric.instrument.Histogram.DynamicRange
import kamon.metric.instrument._

import scala.collection.concurrent.TrieMap
import akka.actor._
import kamon.util.{ Supplier, LazyActorRef, TriemapAtomicGetOrElseUpdate }

import scala.concurrent.duration.FiniteDuration

case class EntityRegistration[T <: EntityRecorder](entity: Entity, recorder: T)

trait Metrics {
  def settings: MetricsSettings
  def shouldTrack(entity: Entity): Boolean
  def shouldTrack(entityName: String, category: String): Boolean =
    shouldTrack(Entity(entityName, category))

  //
  // Histograms registration and removal
  //

  def histogram(name: String): Histogram =
    _registerHistogram(name)

  def histogram(name: String, unitOfMeasurement: UnitOfMeasurement): Histogram =
    _registerHistogram(name, unitOfMeasurement = Some(unitOfMeasurement))

  def histogram(name: String, dynamicRange: DynamicRange): Histogram =
    _registerHistogram(name, dynamicRange = Some(dynamicRange))

  def histogram(name: String, unitOfMeasurement: UnitOfMeasurement, dynamicRange: DynamicRange): Histogram =
    _registerHistogram(name, unitOfMeasurement = Some(unitOfMeasurement), dynamicRange = Some(dynamicRange))

  def histogram(name: String, tags: Map[String, String]): Histogram =
    _registerHistogram(name, tags)

  def histogram(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement): Histogram =
    _registerHistogram(name, tags, Some(unitOfMeasurement))

  def histogram(name: String, tags: Map[String, String], dynamicRange: DynamicRange): Histogram =
    _registerHistogram(name, tags, dynamicRange = Some(dynamicRange))

  def histogram(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement, dynamicRange: DynamicRange): Histogram =
    _registerHistogram(name, tags, Some(unitOfMeasurement), Some(dynamicRange))

  def removeHistogram(name: String): Boolean =
    removeHistogram(name, Map.empty)

  def removeHistogram(name: String, tags: Map[String, String]): Boolean

  private[kamon] def _registerHistogram(name: String, tags: Map[String, String] = Map.empty, unitOfMeasurement: Option[UnitOfMeasurement] = None,
      dynamicRange: Option[DynamicRange] = None): Histogram


  //
  // MinMaxCounter registration and removal
  //

  def minMaxCounter(name: String): MinMaxCounter =
    _registerMinMaxCounter(name)

  def minMaxCounter(name: String, unitOfMeasurement: UnitOfMeasurement): MinMaxCounter =
    _registerMinMaxCounter(name, unitOfMeasurement = Some(unitOfMeasurement))

  def minMaxCounter(name: String, dynamicRange: DynamicRange): MinMaxCounter =
    _registerMinMaxCounter(name, dynamicRange = Some(dynamicRange))

  def minMaxCounter(name: String, refreshInterval: FiniteDuration): MinMaxCounter =
    _registerMinMaxCounter(name, refreshInterval = Some(refreshInterval))

  def minMaxCounter(name: String, unitOfMeasurement: UnitOfMeasurement, dynamicRange: DynamicRange): MinMaxCounter =
    _registerMinMaxCounter(name, unitOfMeasurement = Some(unitOfMeasurement), dynamicRange = Some(dynamicRange))







  def minMaxCounter(name: String, tags: Map[String, String]): MinMaxCounter =
    _registerMinMaxCounter(name, tags)

  def minMaxCounter(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement): MinMaxCounter =
    _registerMinMaxCounter(name, tags, Some(unitOfMeasurement))

  def minMaxCounter(name: String, tags: Map[String, String], dynamicRange: DynamicRange): MinMaxCounter =
    _registerMinMaxCounter(name, tags, dynamicRange = Some(dynamicRange))

  def minMaxCounter(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement, dynamicRange: DynamicRange): MinMaxCounter =
    _registerMinMaxCounter(name, tags, Some(unitOfMeasurement), Some(dynamicRange))

  def removeMinMaxCounter(name: String): Boolean =
    removeHistogram(name, Map.empty)

  def removeMinMaxCounter(name: String, tags: Map[String, String]): Boolean

  private[kamon] def _registerMinMaxCounter(name: String, tags: Map[String, String] = Map.empty, unitOfMeasurement: Option[UnitOfMeasurement] = None,
      dynamicRange: Option[DynamicRange] = None, refreshInterval: Option[FiniteDuration] = None): MinMaxCounter

  //
  // Entities registration and removal
  //

  def entity[T <: EntityRecorder](recorderFactory: EntityRecorderFactory[T], entity: Entity): T

  def entity[T <: EntityRecorder](recorderFactory: EntityRecorderFactory[T], name: String): T =
    entity(recorderFactory, Entity(name, recorderFactory.category))

  def entity[T <: EntityRecorder](recorderFactory: EntityRecorderFactory[T], name: String, tags: Map[String, String]): T =
    entity(recorderFactory, Entity(name, recorderFactory.category, tags))

  def register[T <: EntityRecorder](recorderFactory: EntityRecorderFactory[T], entityName: String): Option[EntityRegistration[T]]
  def register[T <: EntityRecorder](entity: Entity, recorder: T): EntityRegistration[T]
  def unregister(entity: Entity): Unit

  def find(entity: Entity): Option[EntityRecorder]
  def find(name: String, category: String): Option[EntityRecorder]

  def subscribe(filter: SubscriptionFilter, subscriber: ActorRef): Unit =
    subscribe(filter, subscriber, permanently = false)

  def subscribe(category: String, selection: String, subscriber: ActorRef, permanently: Boolean): Unit =
    subscribe(SubscriptionFilter(category, selection), subscriber, permanently)

  def subscribe(category: String, selection: String, subscriber: ActorRef): Unit =
    subscribe(SubscriptionFilter(category, selection), subscriber, permanently = false)

  def subscribe(filter: SubscriptionFilter, subscriber: ActorRef, permanently: Boolean): Unit

  def unsubscribe(subscriber: ActorRef): Unit
  def buildDefaultCollectionContext: CollectionContext
  def instrumentFactory(category: String): InstrumentFactory
}

private[kamon] class MetricsImpl(config: Config) extends Metrics {
  import TriemapAtomicGetOrElseUpdate.Syntax

  private val _trackedEntities = TrieMap.empty[Entity, EntityRecorder]
  private val _subscriptions = new LazyActorRef

  val settings = MetricsSettings(config)

  def shouldTrack(entity: Entity): Boolean =
    settings.entityFilters.get(entity.category).map {
      filter ⇒ filter.accept(entity.name)

    } getOrElse (settings.trackUnmatchedEntities)

  def _registerHistogram(name: String, tags: Map[String, String], unitOfMeasurement: Option[UnitOfMeasurement],
    dynamicRange: Option[DynamicRange]): Histogram = {

    val histogramEntity = Entity(name, "histogram", tags)
    val recorder = _trackedEntities.atomicGetOrElseUpdate(histogramEntity, {
      val factory = instrumentFactory(histogramEntity.category)
      HistogramRecorder(HistogramKey(histogramEntity.category, unitOfMeasurement.getOrElse(UnitOfMeasurement.Unknown)), factory.createHistogram(name, dynamicRange))
    }, _.cleanup)

    recorder.asInstanceOf[HistogramRecorder].instrument
  }

  def removeHistogram(name: String, tags: Map[String, String]): Boolean =
    _trackedEntities.remove(Entity(name, "histogram", tags)).isDefined

  def counter(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement): Counter = {
    val counterEntity = Entity(name, "counter", tags)
    val recorder = _trackedEntities.atomicGetOrElseUpdate(counterEntity, {
      val factory = instrumentFactory(counterEntity.category)
      CounterRecorder(CounterKey(counterEntity.category, unitOfMeasurement), factory.createCounter())
    }, _.cleanup)

    recorder.asInstanceOf[CounterRecorder].instrument
  }

  def minMaxCounter(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement, dynamicRange: Option[DynamicRange],
    refreshInterval: Option[FiniteDuration]): MinMaxCounter = {

    val minMaxCounterEntity = Entity(name, "min-max-counter", tags)
    val recorder = _trackedEntities.atomicGetOrElseUpdate(minMaxCounterEntity, {
      val factory = instrumentFactory(minMaxCounterEntity.category)
      MinMaxCounterRecorder(MinMaxCounterKey(minMaxCounterEntity.category, unitOfMeasurement), factory.createMinMaxCounter(name, dynamicRange, refreshInterval))
    }, _.cleanup)

    recorder.asInstanceOf[MinMaxCounterRecorder].instrument
  }

  def gauge(name: String, tags: Map[String, String], unitOfMeasurement: UnitOfMeasurement, dynamicRange: Option[DynamicRange],
    refreshInterval: Option[FiniteDuration] = None, valueCollector: CurrentValueCollector): Gauge = {

    val gaugeEntity = Entity(name, "gauge", tags)
    val recorder = _trackedEntities.atomicGetOrElseUpdate(gaugeEntity, {
      val factory = instrumentFactory(gaugeEntity.category)
      GaugeRecorder(MinMaxCounterKey(gaugeEntity.category, unitOfMeasurement), factory.createGauge(name, dynamicRange, refreshInterval, valueCollector))
    }, _.cleanup)

    recorder.asInstanceOf[GaugeRecorder].instrument
  }

  def entity[T <: EntityRecorder](recorderFactory: EntityRecorderFactory[T], entity: Entity): T = {
    _trackedEntities.atomicGetOrElseUpdate(entity, {
      recorderFactory.createRecorder(instrumentFactory(recorderFactory.category))
    }, _.cleanup).asInstanceOf[T]
  }

  def register[T <: EntityRecorder](recorderFactory: EntityRecorderFactory[T], entityName: String): Option[EntityRegistration[T]] = {
    val entity = Entity(entityName, recorderFactory.category)

    if (shouldTrack(entity)) {
      val instrumentFactory = settings.instrumentFactories.get(recorderFactory.category).getOrElse(settings.defaultInstrumentFactory)
      val recorder = _trackedEntities.atomicGetOrElseUpdate(entity, recorderFactory.createRecorder(instrumentFactory), _.cleanup).asInstanceOf[T]

      Some(EntityRegistration(entity, recorder))
    } else None
  }

  def register[T <: EntityRecorder](entity: Entity, recorder: T): EntityRegistration[T] = {
    _trackedEntities.put(entity, recorder).map { oldRecorder ⇒
      oldRecorder.cleanup
    }

    EntityRegistration(entity, recorder)
  }

  def unregister(entity: Entity): Unit =
    _trackedEntities.remove(entity).map(_.cleanup)

  def find(entity: Entity): Option[EntityRecorder] =
    _trackedEntities.get(entity)

  def find(name: String, category: String): Option[EntityRecorder] =
    find(Entity(name, category))

  def subscribe(filter: SubscriptionFilter, subscriber: ActorRef, permanent: Boolean): Unit =
    _subscriptions.tell(Subscribe(filter, subscriber, permanent))

  def unsubscribe(subscriber: ActorRef): Unit =
    _subscriptions.tell(Unsubscribe(subscriber))

  def buildDefaultCollectionContext: CollectionContext =
    CollectionContext(settings.defaultCollectionContextBufferSize)

  def instrumentFactory(category: String): InstrumentFactory =
    settings.instrumentFactories.getOrElse(category, settings.defaultInstrumentFactory)

  private[kamon] def collectSnapshots(collectionContext: CollectionContext): Map[Entity, EntitySnapshot] = {
    val builder = Map.newBuilder[Entity, EntitySnapshot]
    _trackedEntities.foreach {
      case (identity, recorder) ⇒ builder += ((identity, recorder.collect(collectionContext)))
    }

    builder.result()
  }

  /**
   *  Metrics Extension initialization.
   */
  private var _system: ActorSystem = null
  private lazy val _start = {
    _subscriptions.point(_system.actorOf(SubscriptionsDispatcher.props(settings.tickInterval, this), "metrics"))
    settings.pointScheduler(DefaultRefreshScheduler(_system.scheduler, _system.dispatcher))
  }

  def start(system: ActorSystem): Unit = synchronized {
    _system = system
    _start
    _system = null
  }
}

private[kamon] object MetricsImpl {

  def apply(config: Config) =
    new MetricsImpl(config)
}

